package tests

import (
	mymapreduce "projet/repartie/mapreduce"
	"os"
	"testing"
)

func TestMapReduceSequential(t *testing.T) {
input := "input_test.txt"
_ = os.WriteFile(input, []byte("foo bar foo baz foo bar"), 0644)
defer os.Remove(input)
    
nReduce := 2
mymapreduce.Sequential("testjob", []string{input}, nReduce, mapF, reduceF)
    
    expected := map[string]string{
        "foo": "3",
        "bar": "2",
        "baz": "1",
    }

    got := make(map[string]string)

    // Read all reduce output files and merge results
    for r := 0; r < nReduce; r++ {
        filename := mymapreduce.MergeName("testjob", r)
        tmp := decodeMapFromFile(t, filename)
        defer os.Remove(filename)
        for k, v := range tmp {
            got[k] = v
        }
    }
    
    assertEqualMaps(t, got, expected)

    mymapreduce.CleanIntermediary("testjob", 1, nReduce)
}
