import com.starrocks.jni.connector.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class TestColumnType {

    @Test
    public void parsePrimitiveType() {
        String s = "int";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.INT);
    }

    @Test
    public void parseArrayType() {
        String s = "array<string>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.ARRAY);
        Assert.assertEquals(t.getChildTypes().size(), 1);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
    }

    @Test
    public void parseMapType() {
        String s = "map<int,string>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.MAP);
        Assert.assertEquals(t.getChildTypes().size(), 2);
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assert.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
    }

    @Test
    public void parseStructType() {
        String s = "struct<a:int,b:string,c:struct<a:int,b:string,c:array<int>>,d:struct<a:array<string>>>";
        ColumnType t = new ColumnType(s);
        Assert.assertEquals(t.getTypeValue(), ColumnType.TypeValue.STRUCT);
        Assert.assertEquals(t.getChildTypes().size(), 4);
        Assert.assertEquals(t.getChildNames().get(3), "d");
        Assert.assertEquals(t.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        Assert.assertEquals(t.getChildTypes().get(1).getTypeValue(), ColumnType.TypeValue.STRING);
        Assert.assertEquals(t.getChildTypes().get(2).getTypeValue(), ColumnType.TypeValue.STRUCT);
        Assert.assertEquals(t.getChildTypes().get(3).getTypeValue(), ColumnType.TypeValue.STRUCT);
        {
            ColumnType c = t.getChildTypes().get(2);
            Assert.assertEquals(c.getChildTypes().size(), 3);
            Assert.assertEquals(c.getChildNames().get(2), "c");
            Assert.assertEquals(c.getChildTypes().get(2).getTypeValue(), ColumnType.TypeValue.ARRAY);
            ColumnType c2 = c.getChildTypes().get(2);
            Assert.assertEquals(c2.getChildTypes().size(), 1);
            Assert.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.INT);
        }
        {
            ColumnType c = t.getChildTypes().get(3);
            Assert.assertEquals(c.getChildTypes().size(), 1);
            Assert.assertEquals(c.getChildNames().get(0), "a");
            Assert.assertEquals(c.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.ARRAY);
            ColumnType c2 = c.getChildTypes().get(0);
            Assert.assertEquals(c2.getChildTypes().size(), 1);
            Assert.assertEquals(c2.getChildTypes().get(0).getTypeValue(), ColumnType.TypeValue.STRING);
        }
    }
}

