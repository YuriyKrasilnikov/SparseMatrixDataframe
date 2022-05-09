# Sparse Matrices Dataframe
## Description
Interact with Spark Dataframe like as with sparse matrices

docker build -t  scala .\.docker

docker run `
	-itd `
	-v C:\Users\yuryk\otus\BDML\SparseMatrixDataframe\:/home/user/sbt/SparseMatrixDataframe `
  --name scala-sbt `
  scala