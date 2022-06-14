use etcd_client::Client;
use etcdmq::Queue;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // tracing_subscriber::fmt()
    //     .with_max_level(Level::TRACE)
    //     .with_span_events(format::FmtSpan::ACTIVE)
    //     // .json()
    //     .init();

    let mut client = Client::connect(["localhost:2379"], None).await.unwrap();
    let mut f = Queue::new(client, "test2".to_string(), None).await.unwrap();
    f.send("testing message").await;
    let msg = f.try_recv().await;
    println!("{:?}", msg);
    sleep(Duration::from_secs(5)).await;
}
