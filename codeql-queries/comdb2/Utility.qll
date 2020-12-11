import cpp
import semmle.code.cpp.dataflow.DataFlow

string fileLineColumn(DataFlow::Node n) {
    exists(
        string filepath, int startline, int startcolumn, int endline, int endcolumn, string relpath
    |
        n.hasLocationInfo(filepath, startline, startcolumn, endline, endcolumn) and
        relpath = n.getLocation().getFile().getRelativePath() and
        result = relpath + ":" + startline + ":" + startcolumn
    )
}
