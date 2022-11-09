# !!这是一个临时仓库!! [venus-cluster#489](https://github.com/ipfs-force-community/venus-cluster/issues/489)

## 概述
新的外部执行器通信通道由三元组组成，使得支持可配置的各种通信方式成为可能。
- Low level underlying IO (Pipe、 TCP、...)
- Framed IO (LengthDelimitedCodec、LinesCodec、...)
- Serded IO (Json, Bincode、...)

### 例:
#### 1. 兼容 venus-cluster v0.5 现有的通行方式 (Pipe + LinesCodec + Json)
```rust
reconnect::<Pipe, _, _>(Path::from("/bin/cat"), repeat(Duration::from_secs(1)))
    .framed(LinesCodec::default())
    .serded(Json::default());
```

#### 2. 使用 tcp + LengthDelimitedCodec + Bincode
```rust
reconnect::<TcpStream, _, _>("127.0.0.1:8964", repeat(Duration::from_secs(1)))
    .framed(LengthDelimitedCodec::default())
    .serded(Bincode::default());
```

## 重试
reconnect 函数第一个参数是 Low level underlying IO 的构造参数(取决于泛型)，第二个参数是重试迭代器。返回值也是属于 Low level underlying IO

### 例:
#### 1. 无限制的每一秒重试一次
```rust
reconnect::<Pipe, _, _>(Path::from("/bin/cat"), repeat(Duration::from_secs(1)))
```

#### 2. 每一秒重试一次最多重试 3 次
```rust
reconnect::<Pipe, _, _>(Path::from("/bin/cat"), repeat(Duration::from_secs(1)).take(3))
```

#### 2. 每一秒重试一次最多重试 3 次
```rust
reconnect::<Pipe, _, _>(Path::from("/bin/cat"), repeat(Duration::from_secs(1)).take(3))
```

#### 3. 使用 ExponentialBackoff 重试算法
```rust
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

let retry_strategy = ExponentialBackoff::from_millis(10)
        .map(jitter) // add jitter to delays
        .take(3);    // limit to 3 retries

reconnect::<Pipe, _, _>(Path::from("/bin/cat"), retry_strategy)
```

### [Example:](./examples/)
#### 斐波拉契数列
Server: 
```
cargo run --example fib-server
```

Client:
```
cargo run --example fib-client -- 20
```