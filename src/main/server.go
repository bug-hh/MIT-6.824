package main

import (
	"6.824/common"
	"fmt"
	"net/http"
	"net/rpc"
)

func main() {
	var ms = new(common.MathService)
	// 注册 RPC 服务
	err := rpc.Register(ms)
	if err != nil {
		fmt.Printf("rpc server register faild, err:%s", err)
	}
	// 将 RPC 服务绑定到 HTTP 服务中去
	rpc.HandleHTTP()

	fmt.Printf("server start ....")
	err = http.ListenAndServe(":9090", nil)

	if err != nil {
		fmt.Printf("listen and server is failed, err:%v\n", err)
	}
}
