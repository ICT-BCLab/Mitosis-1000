# chain

主要定义了区块链的一些数据结构，如Block，BlockHeader以及Transaction等。

## 注意点
由于本客户端采用的是message-oriented架构，因此为了将这些数据结构装载到`message`中，每个数据结构必须实现`payload.Safe`接口，即：

```go
package payload 
type Safe interface {
    Copy() Safe
    MarshalBinary() []byte
}

// 以Block举例
type Block struct {
}

// 注意传入的是对象
func (b Block) Copy() payload.Safe {
    ...
}

func (b Block) MarshalBinary() []byte {
    ...
}
    
```