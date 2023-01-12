package main

import (
	"fmt"
	// "Users\57323\Documents\Yuli\prueba\services"
	"io/ioutil"
	"os"
)

func main() {

	// Get the name of data base and current directory
	nombre_db := os.Args
	if len(nombre_db) < 2 {
		panic("Base de datos no especificada")
	}
	actualPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	actualPath += "/" + nombre_db[1]

	//Declare environment name of bd
	os.Setenv("nombre_db", nombre_db[1])

	// Get the currents files
	archivos, err := ioutil.ReadDir(actualPath)
	if err != nil {
		panic(err)
	}
	if len(archivos) < 1 {
		panic("Archivos no encontrados")
	}

	var archivosList []string
	var directoriosList []string

	for _, archivo := range files {

		if archivo.IsDir() {
			directoriosList = append(directoriosList, archivo.Name())
		} else {
			archivosList = append(archivosList, archivo.Name())
		}
	}

	if len(archivosList) >= 1 {
		functions.ConvertirNdjson(archivosList, actualPath)
	}

	for _, dir := range directoriosList {
		functions.NavegarDirectorios(dir, actualPath)
	}

	functions.EnviarZincSearch()
	fmt.Println("Indexación de archivo logrado")

}
