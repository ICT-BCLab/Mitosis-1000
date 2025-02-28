# eventbus包

通过本包可以实现模块间弱耦合数据传输。该包的设计思路是发布订阅模式，即在模块中，主要存在三个角色：
* Publisher：事件发布者
* Subscriber: 事件订阅监听者
* Broker: 代理人

在本包中，主要有对象`EventBus`，它相当于代理人，每个需要进行模块间数据通信的对象，都应该存储有该对象的指针，但编程实现中不建议直接定义该对象，及如下所示形式。

```go
type Obj struct {
    eb *EventBus
} 
```

而是应该根据对象的实际需求，采用接口定义代替对象定义。

同时，该对象实现了四种接口，`Publisher`、`Listener`、`Subscriber`以及`Broker`接口，其中`Broker`接口是`Publisher`、`Listener`两者的合体。对于只要实现监听的对象，它的定义如下所示：

```go
type Obj struct {
    subscriberID int32
    subscriber *Subscriber
} 

New() {
    subscriberID := subscriber.Subscribe(topics.Topic, Lisenter)
}

```

对于只需要发布事件的对象，它的定义如下所示：

```go
type Obj struct {
    eb *Publisher
} 
```

对于两者都需要的对象，它的定义如下所示：

```go
type Obj struct {
    eb *Broker
} 
```

它主要的事件的传递流程如下所示：
```
                                                                             |--------|
|---------|                                  |-------|                      /|Listener|
|         |                                  |       |                       |--------|
|Publisher|--Publish(eventTopic, message)--->|Broker |------Subscribe(topic)-                  
|         |                                  |       |                       |--------|   
|---------|                                  |-------|                      \|Listener|                                   
                                                                             |--------|
   
```

## Listener接口

 listener主要实现了`Listener`接口，在本包中主要实现了3种Listener，分别是：
 * `CallbackListener`：订阅某个主题的事件后，在接收到某个事件后，直接调用回调函数，执行动作
 * `ChanListener`：订阅某个主题的事件后，在接收到某个事件，将事件信息存储到通道内，由用户自行决定如果处理这些事件。
 * `multiListener`：可以订阅多个主题，能够接收多种主题的事件

该接口主要实现了两种函数：

```go
// 收到事件后如何处理
Notify(message.Message) error
// 关闭接口
Close()
```

## Publisher接口

该接口主要实现了1种函数：

```go
// 发布某主题的事件
Publish(t topics.Topic, m message.Message) []error
```

# Usage

一般情况，程序是全局只存在唯一一个事件发布订阅中心的，在可根据实际需求进行调整。

使用方法如下所示：
```go
// 在main程序中
// 创建事件订阅收发对象
eb := eventbus.New()


// 在需要订阅某个事件的对象中
type needSubscribe struct {
    ...
    subscribeID int32
    subscriber eventbus.Subscriber
}
// 常用的ChanListenr举例，其他的listener一样
writeQueue = make(chan message.Message, 10000)
subscribeID = int32(w.subscriber.Subscribe(topics.Broadcast, eventbus.NewChanListener(w.writeQueue)))

// 在需要发布某个事件的对象中
type needPublish struct {
    publisher eventbus.Publisher
}
// 直接发布事件
publisher.Publish(topics.Broadcast, msg)
```


