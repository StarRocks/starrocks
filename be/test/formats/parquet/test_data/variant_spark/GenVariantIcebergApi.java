// Rewrites the rows of the Spark-written unshredded table with the plain Iceberg 1.12 Java API
// (GenericFileWriterFactory, which infers the shredding layout with RecordVariantShreddingAnalyzer),
// one data file per id range, into the table variant_spark_gen.api_shred.
// Run by gen_variant_spark.sh after gen_variant_spark.sql:
//   java -cp "<spark jars>:<iceberg-spark-runtime-4.1_2.13-1.12.0.jar>" GenVariantIcebergApi.java <warehouse>

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;

public class GenVariantIcebergApi {
    private static final int[][] RANGES = {{1, 6}, {7, 10}, {11, 16}, {17, 21}, {101, 250}, {301, 405}};

    public static void main(String[] args) throws Exception {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), args[0]);
        Table src = catalog.loadTable(TableIdentifier.of("variant_spark_gen", "noshred"));
        TableIdentifier dstId = TableIdentifier.of("variant_spark_gen", "api_shred");
        if (catalog.tableExists(dstId)) {
            catalog.dropTable(dstId, true);
        }
        Table dst = catalog.createTable(dstId, src.schema(), PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "3", "write.parquet.shred-variants", "true"));

        GenericFileWriterFactory writers = new GenericFileWriterFactory.Builder(dst).dataFileFormat(FileFormat.PARQUET).build();
        OutputFileFactory files = OutputFileFactory.builderFor(dst, 1, 1).format(FileFormat.PARQUET).build();
        for (int[] range : RANGES) {
            List<Record> rows = new ArrayList<>();
            try (CloseableIterable<Record> it = IcebergGenerics.read(src)
                    .where(Expressions.and(Expressions.greaterThanOrEqual("id", range[0]),
                            Expressions.lessThanOrEqual("id", range[1])))
                    .build()) {
                it.forEach(r -> rows.add(r.copy()));
            }
            rows.sort(Comparator.comparing(r -> (Integer) r.getField("id")));
            DataWriter<Record> writer = writers.newDataWriter(files.newOutputFile(), PartitionSpec.unpartitioned(), null);
            try (writer) {
                rows.forEach(writer::write);
            }
            DataFile file = writer.toDataFile();
            dst.newAppend().appendFile(file).commit();
            System.out.println(range[0] + "\t" + file.location() + "\t" + file.recordCount());
        }
    }
}
