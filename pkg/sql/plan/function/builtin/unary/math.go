// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type mathFn func(float64) (float64, error)

func math1(length int, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, fn mathFn) error {
	rs := vector.MustFunctionResult[float64](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val, err := fn(v)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Acos(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Acos)
}

func Atan(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Atan)

}

func Cos(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Cos)
}

func Cot(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Cot)
}

func Exp(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Exp)
}

func Ln(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Exp)
}

/*
func Log(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vs) == 1 {
		return math1(vs, proc, momath.Ln)
	}
	if vs[0].IsConstNull() {
		return vector.NewConstNull(*vs[0].GetType(), vs[0].Length(), proc.Mp()), nil
	}
	vals := vector.MustFixedCol[float64](vs[0])
	for i := range vals {
		if vals[i] == float64(1) {
			return nil, moerr.NewInvalidArg(proc.Ctx, "log base", 1)
		}
	}
	v1, err := math1([]*vector.Vector{vs[0]}, proc, momath.Ln)
	if err != nil {
		return nil, moerr.NewInvalidArg(proc.Ctx, "log input", "<= 0")
	}
	v2, err := math1([]*vector.Vector{vs[1]}, proc, momath.Ln)
	if err != nil {
		return nil, moerr.NewInvalidArg(proc.Ctx, "log input", "<= 0")
	}
	return operator.DivFloat[float64]([]*vector.Vector{v2, v1}, proc)
}
*/

func Sin(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Sin)
}

func Sinh(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Sinh)
}

func Tan(vs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return math1(length, vs, result, proc, momath.Tan)
}
