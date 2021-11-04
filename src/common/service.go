package common

import (
	"errors"
	"fmt"
)

type Args struct {
	A, B float32
}

type Result struct {
	Value float32
}

type MathService struct {}


func (s *MathService) Add (args *Args, result *Result) error{
	result.Value = args.A + args.B
	return nil
}


func (s *MathService) Divide(args *Args, result *Result) error{
	if args.B == 0 {
		return errors.New("arge.B is 0")
	}
	fmt.Printf("result: %v", result)
	result.Value = args.A / args.B
	return nil
}