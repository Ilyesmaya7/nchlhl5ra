package tests

import (
	mymapreduce "projet/repartie/mapreduce"
	"testing"
	"os"
	"encoding/json"
)
var jobName = "jobwcount" 

func TestDoMap(t *testing.T) {
	// Créer un fichier temporaire d'entrée
	input := "orange banana banana apple orange banana"
	expectedKeys := map[string]string{}
	expectedKeys["banana"]="3"
	expectedKeys["orange"]="2"
	expectedKeys["apple"]="1"

	inputFile := "test_input.txt"
	file, err := os.Create(inputFile)
	checkErrFatal(t, err, "cannot create input file: %v", err)
	_, err = file.WriteString(input)
	file.Close()
	defer os.Remove(inputFile)

	mapTaskNumber := 555
	nReduce := 10
	// Appeler doMap
	mymapreduce.DoMap(jobName, mapTaskNumber, inputFile, nReduce, mapF)

	gotKeys:= map[string]string{}
	// Lire les fichiers intermédiaires générés
	for r := 0; r < nReduce; r++ {
		fileName := mymapreduce.ReduceName(jobName,mapTaskNumber,r)	
		tmp:=decodeMapFromFile(t, fileName)
		defer os.Remove(fileName)
		for k,v := range tmp{
			gotKeys[k]=v
		}
	}
	assertEqualMaps(t, gotKeys, expectedKeys)
}

func TestDoReduce(t *testing.T) {
	jobName := "job1"
	reduceTaskNumber := 0
	nMap := 2

	// Créer des fichiers intermédiaires simulés produits par doMap
	inputs := [][]mymapreduce.KeyValue{
		{{"apple", "1"}, {"banana", "2"}},
		{{"apple", "1"}, {"orange", "2"}},
	}
	expectedKeys := map[string]string{}
	expectedKeys["banana"]="2"
	expectedKeys["orange"]="2"
	expectedKeys["apple"]="2"
	
	for i := 0; i < nMap; i++ {
		fileName := mymapreduce.ReduceName(jobName, i, reduceTaskNumber)
		file, err := os.Create(fileName)
		defer os.Remove(fileName)
		checkErrFatal(t,err,"cannot create file %s: %v", fileName, err)
		
		enc := json.NewEncoder(file)
		for _, kv := range inputs[i] {
			err := enc.Encode(&kv)
			checkErrFatal(t,err, "cannot encode kv: %v", err)
		}
		file.Close()
	}
	
	// Appeler doReduce
	mymapreduce.DoReduce(jobName, reduceTaskNumber, nMap, reduceF)
	
	// Vérifier le fichier de sortie
	fileName := mymapreduce.MergeName(jobName, reduceTaskNumber)
	defer os.Remove(fileName)
	gotKeys:=decodeMapFromFile(t, fileName)
	
	assertEqualMaps(t, gotKeys,expectedKeys)
}
