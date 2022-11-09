package com.google.cloud.solutions.realtimedash.pipeline;

import java.util.Map;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class AddCompanyDataToMessage extends DoFn<BranchCompanySkuValue, BranchCompanySkuValue> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private final PCollectionView<Map<String, String>> branches;

    public AddCompanyDataToMessage(PCollectionView<Map<String, String>> branches) {
        this.branches = branches;
    }

    private Map<String, String> branchesTable;

  @ProcessElement
  public void ProcessElement(ProcessContext c) {
      branchesTable = c.sideInput(branches);
      if (c.element().getCompany().length()==0) {
      
      String branch = c.element().getBranch();

        // check if the token is a key in the "branches" side input
        if (branchesTable.containsKey(branch)) {
          c.output(
            new BranchCompanySkuValue(
              branch,
              branchesTable.get(branch),
              c.element().getSku(),
              c.element().getValue()
            )
          );
        }
      }

  }
}
