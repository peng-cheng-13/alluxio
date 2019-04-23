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
import org.workflowsim.FileItem;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters.FileType;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Calendar;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a 0 byte file specified by argv. The file will be written to UnderFileSystem.
 */
@ThreadSafe
public final class LoadDaxCommand extends AbstractFileSystemCommand {

  /*Init data structure*/
  private Map<String, String> mOutputFile2Task;
  private Map<String, List<String>> mOutput2InputFiles;
  private Map<String, String> mOutput2OutputFiles;
  private Map<String, Integer> mTaskType2Nums;
  private Map<String, Integer> mTask2ChildNums;

  /**
   * @param fs the filesystem of Alluxio
   */
  public LoadDaxCommand(FileSystem fs) {
    super(fs);
    mOutputFile2Task = new HashMap<>();
    mOutput2InputFiles = new HashMap<String, List<String>>();
    mOutput2OutputFiles = new HashMap<>();
    mTaskType2Nums = new HashMap<>();
    mTask2ChildNums = new HashMap<>();
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
    mFileSystem.defineDax(daxPath, mOutputFile2Task, mOutput2InputFiles,
        mTaskType2Nums, mTask2ChildNums, mOutput2OutputFiles);
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
    List<Task> taskList = tmpParser.getTaskList();
    int jobNum = taskList.size();
    String tmpType = taskList.get(0).getType();
    System.out.println("Current workflow has " + jobNum + " tasks, the first task is " + tmpType);

    int tid;
    /*Parse input workflow*/
    for (tid = 0; tid < jobNum; tid++) {
      int inputFileNum = 0;
      int outputFileNum = 0;
      int childNum = 0;
      int typeNum = 0;
      ArrayList<String> inputFileList = new ArrayList<>();
      Task currentTask = taskList.get(tid);
      String taskName = currentTask.getType();
      /*Parse input and output files*/
      List<FileItem> fileList = currentTask.getFileList();
      for (FileItem file : fileList) {
        /*Num of input files*/
        if (file.getType() == FileType.INPUT) {
          inputFileList.add(file.getName());
          inputFileNum++;
        } else if (file.getType() == FileType.OUTPUT) {
          outputFileNum++;
        }
      }
      /*In case that tasks has the same name but with different input file numbers*/
      taskName = taskName + ":" + inputFileNum;
      /*Mapping between each output file and task*/
      int outputID = 0;
      for (FileItem file : fileList) {
        if (file.getType() == FileType.OUTPUT) {
          mOutputFile2Task.put(file.getName(), taskName);
          /*Mapping between each output file and their input file list*/
          mOutput2InputFiles.put(file.getName(), inputFileList);
          /*id of each putput file*/
          String outinfo = String.valueOf(outputFileNum) + ":" + String.valueOf(outputID);
          //System.out.println("Id of " + file.getName() + "is" + outinfo);
          mOutput2OutputFiles.put(file.getName(), outinfo);
          outputID++;
        }
      }

      /*Num of tasks with the same type and same num of input files*/
      if (!mTaskType2Nums.containsKey(taskName)) {
        mTaskType2Nums.put(taskName, 1);
      } else {
        typeNum = mTaskType2Nums.get(taskName) + 1;
        mTaskType2Nums.put(taskName, typeNum);
      }
      /*Mapping between task and num of childs*/
      childNum = currentTask.getChildList().size();
      if (!mTask2ChildNums.containsKey(taskName)) {
        mTask2ChildNums.put(taskName, childNum);
      }
    }
  }

}
