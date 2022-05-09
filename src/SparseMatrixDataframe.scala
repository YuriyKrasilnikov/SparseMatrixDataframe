import org.apache.spark.sql.{DataFrame}

trait MultiplySparseMatrixDataframe{
    
    import org.apache.spark.sql.expressions.Aggregator
    
    protected object MatrixMultiply extends Aggregator[(Long, Double, Long, Double), Double, Double] {
    
        import org.apache.spark.sql.{Encoder, Encoders}
        
        def zero :Double = 0.0 // Init the buffer
      
        def reduce(buffer: Double, x: (Long, Double, Long, Double)) :Double = {
            //System.out.println(s"buffer=$buffer x=$x")
            if (x._1 == x._3){
                buffer+x._2*x._4
            }else{
                buffer
            }
            
        }
    
        def merge(a: Double, b: Double) :Double = {
            a+b
        }
    
        def finish(r: Double) :Double = r
      
        def bufferEncoder: Encoder[Double] = Encoders.scalaDouble
    
        def outputEncoder: Encoder[Double] = Encoders.scalaDouble
        
    }
    
    protected val matrixMultiplyUdaf = udaf(MatrixMultiply)
    
    protected def multiplicationMatrix(
        lDf :DataFrame,
        rDf :DataFrame,
        colsName :(String, String, String) 
    ) :DataFrame = {
       lDf.as("l").join(
            rDf.as("r"),
            lDf(colsName._2) ===  rDf(colsName._1)
        ).groupBy("l."+colsName._1, "r."+colsName._2)
        .agg(
            matrixMultiplyUdaf(
                col("l."+colsName._2),
                col("l."+colsName._3),
                col("r."+colsName._1),
                col("r."+colsName._3)
            ).as(colsName._3)
        ) 
    }
    
}

trait SumSparseMatrixDataframe {
    
    // Sum
    protected def sumMatrix(
        lDf :DataFrame,
        rDf :DataFrame,
        colsName :(String, String, String) 
    ) :DataFrame = {
       lDf.as("l").join(
            rDf.as("r"),
            lDf(colsName._1) ===  rDf(colsName._1) && lDf(colsName._2) ===  rDf(colsName._2),
            "Outer"
        ).select(
            when(col("l."+colsName._1).isNull, col("r."+colsName._1)).otherwise(col("l."+colsName._1)).as(colsName._1),
            when(col("l."+colsName._2).isNull, col("r."+colsName._2)).otherwise(col("l."+colsName._2)).as(colsName._2),
            (
                when(col("l."+colsName._3).isNull, 0.0).otherwise(col("l."+colsName._3))+
                    when(col("r."+colsName._3).isNull, 0.0).otherwise(col("r."+colsName._3))
            ).as(colsName._3)
        )
    }
    
}

trait TransposeSparseMatrixDataframe {
    
    // Transpose
    protected def transpose(df :DataFrame) :DataFrame = {
        df.select(
            col(df.columns(1)).as(df.columns(0)),
            col(df.columns(0)).as(df.columns(1)),
            col(df.columns(2))
        )
    }
    
}

trait SparseMatrixDataframeToMatrix {

    // To Matrix
    protected def getBlockMatrix(df :DataFrame) = {
        import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
        
        new CoordinateMatrix(
            df.rdd.map(
                m => MatrixEntry(m.getLong(0),m.getLong(1),m.getDouble(2))
            )
        ).toBlockMatrix
    }
    
}

case class SparseMatrixDataframe(data: DataFrame) extends
    MultiplySparseMatrixDataframe with
    TransposeSparseMatrixDataframe with
    SumSparseMatrixDataframe with
    SparseMatrixDataframeToMatrix 
    {
    
    // constants
    // ---
    // columns name
    private val rowName = "row"
    private val colName = "col"
    private val valueName = "value"
    private val colsName = (rowName, colName, valueName)
    
    //Matrix slice
    def apply[A,B](rowLs: A, colLs: B)(implicit
        evA: (::.type with Int with List[Int]) <:< A,
        evB: (::.type with Int with List[Int]) <:< B
    ):SparseMatrixDataframe = {
            SparseMatrixDataframe(
                this.filter(
                    this.filter(this.df, rowLs, rowName),
                    colLs,
                    colName
                )
            )
    }
    
    private def filter[A](data: DataFrame, rule:A, name:String): DataFrame = {
        rule match {
                case _: ::.type => data
                case i: Int => data.filter(col(name) === i)
                case lst: List[Int] => data.filter(col(name).isin(lst: _*))
        }
    }
    
    //Matrix multiplication
    def *(that: SparseMatrixDataframe) :SparseMatrixDataframe = SparseMatrixDataframe(
        this.multiplicationMatrix(
            lDf=this.df,
            rDf=that.df,
            colsName=colsName
        )
    )
    
    //Multiplying matrix by a number
    def *[A](that: A) :SparseMatrixDataframe = SparseMatrixDataframe(
        this.df.withColumn(valueName, col(valueName)*that)
    )
    
    //Dividing matrix by a number
    def /[A](that: A) :SparseMatrixDataframe = SparseMatrixDataframe(
        this.df.withColumn(valueName, col(valueName)/that)
    )
    
    //Matrix addition
    def +(that: SparseMatrixDataframe) :SparseMatrixDataframe = SparseMatrixDataframe(
        this.sumMatrix(
            lDf=this.df,
            rDf=that.df,
            colsName=colsName
        )
    )
    
    //Transpose
    def T = SparseMatrixDataframe(this.transpose(this.df))
    
    //to Matrix
    def toBlockMatrix = this.getBlockMatrix(this.df)
    def toLocalMatrix = this.toBlockMatrix.toLocalMatrix
    
    //show
    def show = this.df.show
    
    // init
    protected def correctedDF(df :DataFrame, columnsName :(String, String, String)) :DataFrame = {
        df.select(df.columns.slice(0,3).zipWithIndex.map{ case (column, i) => {
                i match {
                    case 0  => col(column).cast("Long").as(columnsName._1)
                    case 1  => col(column).cast("Long").as(columnsName._2)
                    case 2  => col(column).cast("Double").as(columnsName._3)
                    //case _  => col(column)
                }
            }}:_*)
    }
    
    val df = correctedDF(data, this.colsName)
    
}

implicit class NumberToSparseMatrixDataframe[A](number: A) {
    def *(that: SparseMatrixDataframe) :SparseMatrixDataframe = {
        that * this.number
    }
}
