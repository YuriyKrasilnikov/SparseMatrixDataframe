val data = Seq(
  (2, 1, 1), (4, 1, 1),
  (1, 2, 1), (2, 3, 1), (3, 3, 1)
)

val df1 = data.toDF
val df2 = df1.select("_2","_1","_3")

val M1 = SparseMatrixDataframe(df1)
val M2 = SparseMatrixDataframe(df2)
val M3 = M1*M2

M3.show

println(M3.toLocalMatrix)
println()

val bM1 = M1.toBlockMatrix
val bM2 = M2.toBlockMatrix

println(bM1.multiply(bM2).toLocalMatrix)