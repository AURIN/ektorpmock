package org.ektorpmock;

import org.ektorp.ViewQuery;
import org.ektorp.support.DesignDocument;

import java.util.List;

public interface ViewEvaluator {
    List evaluateView(DesignDocument.View view, ViewQuery query, List data);
}
