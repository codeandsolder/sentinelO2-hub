mod transfer;

use super::*;
use sentinel0_hub_core::{DirectRequestInput, prepare_direct_request};
use sentinel0_proto::HostInfo;
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message as ClientMessage, client::IntoClientRequest},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type AgentSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn state() -> TestResult<AppState> {
    let store = NativeStore::open(":memory:")?;
    Ok(AppState {
        hub: Arc::new(Hub {
            enrollment_token: "enrollment-test".to_owned(),
            api_token: "api-test".to_owned(),
            registry: RwLock::new(store.load_host_registry()?),
            sessions: RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            transfer_binary_waiters: Mutex::new(HashMap::new()),
            transfer_ack_waiters: Mutex::new(HashMap::new()),
            store: StdMutex::new(store),
        }),
    })
}

async fn server(state: AppState) -> TestResult<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let app = Router::new()
        .route("/agent/connect", get(agent_connect))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((address, handle))
}

async fn send_agent(socket: &mut AgentSocket, message: &Message) -> TestResult {
    socket
        .send(ClientMessage::Text(serde_json::to_string(message)?.into()))
        .await?;
    Ok(())
}

async fn read_agent(socket: &mut AgentSocket) -> TestResult<Message> {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await?
        .ok_or("agent WebSocket closed")??;
    let ClientMessage::Text(raw) = frame else {
        return Err(format!("expected agent text frame, got {frame:?}").into());
    };
    Ok(serde_json::from_str(&raw)?)
}

async fn connect_agent(address: SocketAddr, host_id: &str) -> TestResult<(AgentSocket, String)> {
    let mut request = format!("ws://{address}/agent/connect").into_client_request()?;
    request
        .headers_mut()
        .insert("authorization", "Bearer enrollment-test".parse()?);
    let (mut socket, _) = connect_async(request).await?;
    let hello = Message::Hello {
        protocol_version: "1.0".to_owned(),
        agent_version: "test".to_owned(),
        agent_name: None,
        host: Box::new(HostInfo {
            id: host_id.to_owned(),
            hostname: format!("{host_id}.test"),
            os: "linux".to_owned(),
            kernel: None,
            arch: None,
            cpu_model: None,
            cpu_cores: None,
            mem_total_bytes: None,
            disk_total_bytes: None,
            machine_type: None,
            distro: None,
            config_summary: None,
        }),
        capabilities: Vec::new(),
        preferred_profile: None,
    };
    send_agent(&mut socket, &hello).await?;
    let Message::Welcome { session_id, .. } = read_agent(&mut socket).await? else {
        return Err("expected Welcome frame".into());
    };
    Ok((socket, session_id))
}

async fn wait_for_session(state: &AppState, host_id: &str, session_id: &str) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state
                .hub
                .sessions
                .read()
                .await
                .get(host_id)
                .is_some_and(|session| session.session_id == session_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn responses_are_correlated_to_the_dispatched_host() -> TestResult {
    let state = state()?;
    let (address, server) = server(state.clone()).await?;
    let (mut alpha, alpha_session) = connect_agent(address, "alpha").await?;
    let (mut beta, beta_session) = connect_agent(address, "beta").await?;
    wait_for_session(&state, "alpha", &alpha_session).await?;
    wait_for_session(&state, "beta", &beta_session).await?;

    let prepared = {
        let registry = state.hub.registry.read().await;
        prepare_direct_request(
            &registry,
            DirectRequestInput {
                op_name: "exec".to_owned(),
                host_selector: Some("alpha".to_owned()),
                payload: json!({"command": "true"}),
                client_request_id: Some("response-host-integration-test".to_owned()),
            },
        )?
    };
    let repeated = prepared.clone();
    let dispatch_state = state.clone();
    let dispatch =
        tokio::spawn(async move { dispatch_prepared_request(&dispatch_state, prepared).await });
    let Message::Request { id, .. } = read_agent(&mut alpha).await? else {
        return Err("expected dispatched Request frame".into());
    };
    let impostor = Message::Response {
        id: id.clone(),
        ok: true,
        result: Some([("output".to_owned(), json!("wrong host"))].into()),
        error: None,
    };
    send_agent(&mut beta, &impostor).await?;

    // A round-trip Ping guarantees the preceding response was processed.
    let timestamp = Utc::now();
    send_agent(&mut beta, &Message::Ping { timestamp }).await?;
    assert!(matches!(read_agent(&mut beta).await?, Message::Pong { .. }));
    assert!(
        !dispatch.is_finished(),
        "another host completed alpha's request"
    );

    let valid = Message::Response {
        id,
        ok: true,
        result: Some([("output".to_owned(), json!("correct host"))].into()),
        error: None,
    };
    send_agent(&mut alpha, &valid).await?;
    let response = tokio::time::timeout(Duration::from_secs(5), dispatch).await??;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(body["result"]["output"], "correct host");

    let replay = dispatch_prepared_request(&state, repeated).await;
    assert_eq!(replay.status(), StatusCode::OK);
    let body = axum::body::to_bytes(replay.into_body(), 64 * 1024).await?;
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(body["replayed"], true);
    assert_eq!(body["result"]["output"], "correct host");
    server.abort();
    Ok(())
}

#[tokio::test]
async fn late_response_only_completes_the_original_hosts_request() -> TestResult {
    let state = state()?;
    {
        let store = state
            .hub
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            store.begin_idempotency("late-client", "fingerprint", "hreq_late", "alpha")?,
            BeginIdempotency::Start
        );
    }
    let response = Message::Response {
        id: "hreq_late".to_owned(),
        ok: true,
        result: Some([("output".to_owned(), json!("valid result"))].into()),
        error: None,
    };
    let raw = serde_json::to_string(&response)?;
    handle_agent_text(&state, "beta", &raw).await;
    {
        let store = state
            .hub
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            store
                .pending_client_request_id("hreq_late", "alpha")?
                .as_deref(),
            Some("late-client")
        );
    }
    handle_agent_text(&state, "alpha", &raw).await;
    let replay = {
        let store = state
            .hub
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        store.begin_idempotency("late-client", "fingerprint", "new_request", "alpha")?
    };
    let BeginIdempotency::Replay { status, body } = replay else {
        return Err("expected completed late response".into());
    };
    assert_eq!(status, 200);
    assert_eq!(body["result"]["output"], "valid result");
    Ok(())
}

#[tokio::test]
async fn replaced_connection_cannot_disconnect_the_new_session() -> TestResult {
    let state = state()?;
    let (address, server) = server(state.clone()).await?;
    let (mut old, old_session) = connect_agent(address, "same-host").await?;
    wait_for_session(&state, "same-host", &old_session).await?;

    let (mut current, current_session) = connect_agent(address, "same-host").await?;
    wait_for_session(&state, "same-host", &current_session).await?;
    assert_ne!(old_session, current_session);

    let _ = old.close(None).await;
    // The current socket must remain live despite the superseded close.
    let timestamp = Utc::now();
    send_agent(&mut current, &Message::Ping { timestamp }).await?;
    assert!(matches!(
        read_agent(&mut current).await?,
        Message::Pong { .. }
    ));
    let sessions = state.hub.sessions.read().await;
    assert_eq!(
        sessions
            .get("same-host")
            .map(|session| session.session_id.as_str()),
        Some(current_session.as_str())
    );
    drop(sessions);
    assert!(
        state
            .hub
            .registry
            .read()
            .await
            .resolve(Some("same-host"))
            .is_ok()
    );
    server.abort();
    Ok(())
}
