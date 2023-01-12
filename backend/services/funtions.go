/*
Funciones para convertir los archivos en el formato
nd_json y subirlos a la bases de datos
*/
package functions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

func ManejarErrores(err error) {
	if err != nil {
		panic(err)
	}
}

/*
Navegar por los directorios y archivos para crear los índices de la base de datos.
*/
func NavegarDirectorios(nombreDir string, actualPath string) {

	actualPath += "/" + nombreDir

	// Obtener archivos
	archivos, err := ioutil.ReadDir(actualPath)
	ManejarErrores(err)

	if len(archivos) < 1 {
		panic("No files found")
	}

	// Lista con los nombres de los archivos y directorios
	var archivosList []string
	var directoriosList []string

	for _, archivo := range archivos {

		if archivo.IsDir() {
			directoriosList = append(directoriosList, archivo.Name())
		} else {
			archivosList = append(archivosList, archivo.Name())
		}
	}

	if len(archivosList) >= 1 {
		ConvertirNdjson(archivosList, actualPath)
	}

	for _, dir := range directoriosList {
		NavegarDirectorios(dir, actualPath)
	}

	if len(directoriosList) == 0 {
		return
	}
}

/*Convierte la data en un archivo ndjson*/
func EscribirArchivo(direct1 []byte, direct2 []byte) {

	if _, err := os.Stat(os.Getenv("nombre_db") + ".ndjson"); err == nil {
		//File exists
		f, err := os.OpenFile(os.Getenv("nombre_db")+".ndjson", os.O_APPEND|os.O_WRONLY, 0660)
		ManejarErrores(err)
		str := string(direct1)
		_, err = fmt.Fprint(f, str, "\n")
		ManejarErrores(err)
		str2 := string(direct2)
		_, err = fmt.Fprint(f, str2, "\n")
		ManejarErrores(err)

		defer f.Close()

	} else {
		//File does not exist
		f, err := os.Create(os.Getenv("nombre_db") + ".ndjson")
		ManejarErrores(err)
		str := string(direct1)
		_, err = fmt.Fprint(f, str, "\n")
		ManejarErrores(err)
		str2 := string(direct2)
		_, err = fmt.Fprint(f, str2, "\n")
		ManejarErrores(err)

		defer f.Close()
	}
}

/*
Se toma el nombre de los archivos en directorios y se crea el ndjson
*/
func ConvertirNdjson(nombresArchivos []string, path string) {

	splitIndex := strings.Split(path, "/")
	var nombreIndex string

	if len(splitIndex) >= 2 {
		nombreIndex1 := splitIndex[len(splitIndex)-2]
		nombreIndex1 = strings.TrimPrefix(nombreIndex1, "_")
		nombreIndex = nombreIndex1 + "." + splitIndex[len(splitIndex)-1]
	} else {
		nombreIndex = splitIndex[len(splitIndex)-1]
		nombreIndex = strings.TrimPrefix(nombreIndex, "_")
	}

	var cont int64 = 0
	for _, nombreArchivo := range nombresArchivos {

		MyArhivo, err := os.Stat(path + "/" + nombreArchivo)
		if err != nil {
			fmt.Println("El archivos no existe")
		}
		cont += MyArhivo.Size()
	}

	if cont > 700000 {
		chunkSlice(nombresArchivos, len(nombresArchivos)/2, path)
		return
	}

	//compilación del primer diccionario para el formato masivo de documentos
	direct1 := map[string]map[string]string{
		"index": {
			"_index": os.Getenv("name_bd"),
		},
	}

	to_json, err := json.Marshal(direct1)
	ManejarErrores(err)

	//build the second dictionary
	direct2 := make(map[string]string)

	for _, nombre := range nombresArchivos {

		content, err := ioutil.ReadFile(path + "/" + nombre)
		ManejarErrores(err)
		//Converir a string
		str_content := string(content)

		direct2[nombreIndex+"."+nombre] = str_content
	}

	to_json2, err := json.Marshal(direct2)
	ManejarErrores(err)

	EscribirArchivo(to_json, to_json2)
}

func chunkSlice(slice []string, chunkSize int, path string) {
	var chunks [][]string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}

	for _, chunk := range chunks {
		ConvertirNdjson(chunk, path)
	}
}

func EnviarZincSearch() {

	//Post_zincsearch
	archivoEncontrado, err := ioutil.ReadFile(os.Getenv("name_bd") + ".ndjson")
	ManejarErrores(err)

	h := http.Client{}
	req, err := http.NewRequest("POST", "http://localhost:4080/api/_bulk", bytes.NewBuffer(archivoEncontrado))
	ManejarErrores(err)

	req.SetBasicAuth("admin", "Complexpass#123")
	r, err := h.Do(req)
	ManejarErrores(err)

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(body))
}
