package workload

import "fmt"

func FileName(i int) string {
	return fmt.Sprintf("file-%05d.dat", i)
}
