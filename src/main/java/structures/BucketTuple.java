package structures;

import java.io.Serializable;

public class BucketTuple implements Comparable<BucketTuple>, Serializable {
    private static final long serialVersionUID = 7740597512347961125L;

    private int pageNumber;
    private int rowNumber;

    public BucketTuple(int pageNumber, int rowNumber) {
        this.pageNumber = pageNumber;
        this.rowNumber = rowNumber;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getRowNumber() {
        return rowNumber;
    }

    @Override
    public int compareTo(BucketTuple bucketTuple) {
        int comp = Integer.compare(this.pageNumber, bucketTuple.getPageNumber());

        if (comp != 0) {
            return comp;
        }

        return Integer.compare(this.rowNumber, bucketTuple.getRowNumber());
    }
}
