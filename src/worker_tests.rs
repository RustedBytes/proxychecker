use super::*;

#[test]
fn panic_and_errors_are_reported() {
    assert_eq!(
        catch_worker_failure(|| panic!("test")),
        Err("proxy worker panicked".into())
    );
    assert_eq!(
        catch_worker_failure(|| Err("setup failed".into())),
        Err("setup failed".into())
    );
    assert!(catch_worker_failure(|| Ok(())).is_ok());
}

#[tokio::test]
async fn cancellation_interrupts_a_backpressure_wait() {
    let (cancel, cancelled) = watch::channel(false);
    let credits = Semaphore::new(0);
    let waiting = async {
        tokio::select! {
            () = wait_for_cancellation(cancelled) => true,
            _ = credits.acquire() => false,
        }
    };
    cancel.send(true).unwrap();
    assert!(waiting.await);
}

#[tokio::test]
async fn cancellation_wakes_on_signal_and_sender_loss() {
    let (cancel, cancelled) = watch::channel(false);
    let task = tokio::spawn(wait_for_cancellation(cancelled));
    tokio::task::yield_now().await;
    cancel.send(true).unwrap();
    task.await.unwrap();
    let (cancel, cancelled) = watch::channel(false);
    drop(cancel);
    wait_for_cancellation(cancelled).await;
}
