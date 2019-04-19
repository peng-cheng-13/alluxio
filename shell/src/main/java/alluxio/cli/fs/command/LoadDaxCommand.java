/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import org.workflowsim.WorkflowParser;
import org.workflowsim.Task;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Calendar;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a 0 byte file specified by argv. The file will be written to UnderFileSystem.
 */
@ThreadSafe
public final class LoadDaxCommand extends AbstractFileSystemCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public LoadDaxCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "loadDax";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String daxPath = args[0];
    parseWorkflow(daxPath);
    mFileSystem.defineDax(daxPath);
    System.out.println(daxPath + " has been parsed");
    return 0;
  }

  @Override
  public String getUsage() {
    return "loadDax <path to workflow discription file (*.dax)>";
  }

  @Override
  public String getDescription() {
    return "Parse workflow discription file in dax format";
  }

  /**
   * Parse workflow description file.
   * @param daxPath the path of workflow description file
   */
  private void parseWorkflow(String daxPath) {
    File daxFile = new File(daxPath);
    if (!daxFile.exists()) {
      System.out.println(
          "Warning: Please replace daxPath with the physical path in your working environment!");
      System.exit(0);
    }
    int vmNum = 20;
    Parameters.SchedulingAlgorithm schmethod = Parameters.SchedulingAlgorithm.MINMIN;
    Parameters.PlanningAlgorithm plnmethod = Parameters.PlanningAlgorithm.INVALID;
    ReplicaCatalog.FileSystem filesystem = ReplicaCatalog.FileSystem.SHARED;
    OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
    ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
    ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);
    Parameters.init(vmNum, daxPath, null,
        null, op, cp, schmethod, plnmethod, null, 0);
    ReplicaCatalog.init(filesystem);
    Calendar calendar = Calendar.getInstance();
    WorkflowParser tmpParser = new WorkflowParser(daxPath);
    tmpParser.parse();
    List<Task> mtask = tmpParser.getTaskList();
    int jobNum = mtask.size();
    String tmpType = mtask.get(0).getType();
    System.out.println("Current workflow has " + jobNum + " tasks, the first task is " + tmpType);
  }

}
