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

package external

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type externalTestCase struct {
	arg      *Argument
	types    []types.Type
	proc     *process.Process
	cancel   context.CancelFunc
	format   string
	jsondata string
}

var (
	cases         []externalTestCase
	defaultOption = []string{"filepath", "abc", "format", "jsonline", "jsondata", "array"}
)

func newTestCase(all bool, format, jsondata string) externalTestCase {
	proc := testutil.NewProcess()
	proc.FileService = testutil.NewFS()
	ctx, cancel := context.WithCancel(context.Background())
	return externalTestCase{
		proc:  proc,
		types: []types.Type{types.T_int8.ToType()},
		arg: &Argument{
			Es: &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx: ctx,
				},
				ExParam: ExParam{
					Fileparam: &ExFileparam{},
					Filter:    &FilterParam{},
				},
			},
		},
		cancel:   cancel,
		format:   format,
		jsondata: jsondata,
	}
}

func init() {
	cases = []externalTestCase{
		newTestCase(true, tree.CSV, ""),
		newTestCase(true, tree.JSONLINE, tree.OBJECT),
		newTestCase(true, tree.JSONLINE, tree.ARRAY),
	}
}

func Test_String(t *testing.T) {
	buf := new(bytes.Buffer)
	String(cases[0].arg, buf)
}

func Test_Call(t *testing.T) {
	convey.Convey("external Call", t, func() {
		for _, tcs := range cases {
			param := tcs.arg.Es
			extern := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Filepath: "",
					Tail: &tree.TailParameter{
						IgnoredLines: 0,
					},
					Format: tcs.format,
				},
				ExParam: tree.ExParam{
					FileService: tcs.proc.FileService,
					JsonData:    tcs.jsondata,
					Ctx:         context.Background(),
				},
			}
			param.Extern = extern
			param.Fileparam.End = false
			param.FileList = []string{"abc.txt"}
			param.FileOffset = [][2]int{{0, -1}}
			param.FileSize = []int64{1}
			end, err := Call(1, tcs.proc, tcs.arg, false, false)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(end, convey.ShouldBeFalse)

			param.Fileparam.End = false
			end, err = Call(1, tcs.proc, tcs.arg, false, false)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end, convey.ShouldBeTrue)

			param.Fileparam.End = true
			end, err = Call(1, tcs.proc, tcs.arg, false, false)
			convey.So(err, convey.ShouldBeNil)
			convey.So(end, convey.ShouldBeTrue)
			require.Equal(t, int64(0), tcs.proc.Mp().CurrNB())
		}
	})
}

func Test_getCompressType(t *testing.T) {
	convey.Convey("getCompressType succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.GZIP,
			},
			ExParam: tree.ExParam{
				Ctx: context.Background(),
			},
		}
		compress := getCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, param.CompressType)

		param.CompressType = tree.AUTO
		param.Filepath = "a.gz"
		compress = getCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.GZIP)

		param.Filepath = "a.bz2"
		compress = getCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.BZIP2)

		param.Filepath = "a.lz4"
		compress = getCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.LZ4)

		param.Filepath = "a.csv"
		compress = getCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)

		param.Filepath = "a"
		compress = getCompressType(param, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)
	})
}

func Test_getUnCompressReader(t *testing.T) {
	convey.Convey("getUnCompressReader succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.NOCOMPRESS,
			},
			ExParam: tree.ExParam{
				Ctx: context.Background(),
			},
		}
		read, err := getUnCompressReader(param, param.Filepath, nil)
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.BZIP2
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.FLATE
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZ4
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZW
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		param.CompressType = "abc"
		read, err = getUnCompressReader(param, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestReadDirSymlink(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	// create a/b/c
	err := os.MkdirAll(filepath.Join(root, "a", "b", "c"), 0755)
	assert.Nil(t, err)

	// write a/b/c/foo
	err = os.WriteFile(filepath.Join(root, "a", "b", "c", "foo"), []byte("abc"), 0644)
	assert.Nil(t, err)

	// symlink a/b/d to a/b/c
	err = os.Symlink(
		filepath.Join(root, "a", "b", "c"),
		filepath.Join(root, "a", "b", "d"),
	)
	assert.Nil(t, err)

	// read a/b/d/foo
	fooPathInB := filepath.Join(root, "a", "b", "d", "foo")
	files, _, err := plan2.ReadDir(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: fooPathInB,
		},
		ExParam: tree.ExParam{
			Ctx: ctx,
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(files))
	assert.Equal(t, fooPathInB, files[0])

	path1 := root + "/a//b/./../b/c/foo"
	files1, _, err := plan2.ReadDir(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: path1,
		},
		ExParam: tree.ExParam{
			Ctx: ctx,
		},
	})
	assert.Nil(t, err)
	pathWant1 := root + "/a/b/c/foo"
	assert.Equal(t, 1, len(files1))
	assert.Equal(t, pathWant1, files1[0])
}

func Test_fliterByAccountAndFilename(t *testing.T) {
	type args struct {
		node     *plan.Node
		proc     *process.Process
		fileList []string
		fileSize []int64
	}

	files := []struct {
		date types.Date
		path string
		size int64
	}{
		{738551, "etl:/sys/logs/2023/02/01/filepath", 1},
		{738552, "etl:/sys/logs/2023/02/02/filepath", 2},
		{738553, "etl:/sys/logs/2023/02/03/filepath", 3},
		{738554, "etl:/sys/logs/2023/02/04/filepath", 4},
		{738555, "etl:/sys/logs/2023/02/05/filepath", 5},
		{738556, "etl:/sys/logs/2023/02/06/filepath", 6},
	}

	toPathArr := func(files []struct {
		date types.Date
		path string
		size int64
	}) []string {
		fileList := make([]string, len(files))
		for idx, f := range files {
			fileList[idx] = f.path
		}
		return fileList
	}
	toSizeArr := func(files []struct {
		date types.Date
		path string
		size int64
	}) []int64 {
		fileSize := make([]int64, len(files))
		for idx, f := range files {
			fileSize[idx] = f.size
		}
		return fileSize
	}

	fileList := toPathArr(files)
	fileSize := toSizeArr(files)

	equalDate2DateFid := function.EncodeOverloadID(function.EQUAL, 14)
	lessDate2DateFid := function.EncodeOverloadID(function.LESS_THAN, 14)
	mologdateFid := function.EncodeOverloadID(function.MO_LOG_DATE, 0)
	tableName := "dummy_table"

	mologdateConst := func(idx int) *plan.Expr {
		return &plan.Expr{
			Typ: &plan.Type{
				Id: int32(types.T_date),
			},
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Dateval{
						Dateval: int32(files[idx].date),
					},
				},
			},
		}
	}
	mologdateFunc := func() *plan.Expr {
		return &plan.Expr{
			Typ: &plan.Type{
				Id: int32(types.T_bool),
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{Obj: mologdateFid, ObjName: "mo_log_date"},
					Args: []*plan.Expr{
						{
							Typ: nil,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: 0,
									ColPos: 0,
									Name:   tableName + "." + catalog.ExternalFilePath,
								},
							},
						},
					},
				},
			},
		}
	}

	nodeWithFunction := func(expr *plan.Expr_F) *plan.Node {
		return &plan.Node{
			NodeType: plan.Node_EXTERNAL_SCAN,
			Stats:    &plan.Stats{},
			TableDef: &plan.TableDef{
				TableType: "func_table",
				TblFunc: &plan.TableFunction{
					Name: tableName,
				},
				Cols: []*plan.ColDef{
					{
						Name: catalog.ExternalFilePath,
						Typ: &plan.Type{
							Id:    int32(types.T_varchar),
							Width: types.MaxVarcharLen,
							Table: tableName,
						},
					},
				},
			},
			FilterList: []*plan.Expr{
				{
					Typ: &plan.Type{
						Id: int32(types.T_bool),
					},
					Expr: expr,
				},
			},
		}
	}

	tests := []struct {
		name  string
		args  args
		want  []string
		want1 []int64
	}{
		{
			name: "mo_log_date_20230205",
			args: args{
				node: nodeWithFunction(&plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{Obj: equalDate2DateFid, ObjName: "="},
						Args: []*plan.Expr{
							mologdateConst(5),
							mologdateFunc(),
						},
					},
				}),
				proc:     testutil.NewProc(),
				fileList: fileList,
				fileSize: fileSize,
			},
			want:  []string{files[5].path},
			want1: []int64{files[5].size},
		},
		{
			name: "mo_log_date_gt_20230202",
			args: args{
				node: nodeWithFunction(&plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{Obj: lessDate2DateFid, ObjName: "<"},
						Args: []*plan.Expr{
							mologdateConst(2),
							mologdateFunc(),
						},
					},
				}),
				proc:     testutil.NewProc(),
				fileList: fileList,
				fileSize: fileSize,
			},
			want:  toPathArr(files[3:]),
			want1: toSizeArr(files[3:]),
		},
		{
			name: "mo_log_date_lt_20230202",
			args: args{
				node: nodeWithFunction(&plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{Obj: lessDate2DateFid, ObjName: "<"},
						Args: []*plan.Expr{
							mologdateFunc(),
							mologdateConst(2),
						},
					},
				}),
				proc:     testutil.NewProc(),
				fileList: fileList,
				fileSize: fileSize,
			},
			want:  toPathArr(files[:2]),
			want1: toSizeArr(files[:2]),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := filterByAccountAndFilename(context.TODO(), tt.args.node, tt.args.proc, tt.args.fileList, tt.args.fileSize)
			require.Nil(t, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.want1, got1)
		})
	}
}
