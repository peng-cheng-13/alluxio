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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Delete attribute to the specified file.
 */
@ThreadSafe
public final class DeleteAttributeCommand extends WithWildCardPathCommand {

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("set attributes recursively")
          .build();

  private static final String REMOVE_UNCHECKED_OPTION_CHAR = "U";
  private static final Option REMOVE_UNCHECKED_OPTION =
      Option.builder(REMOVE_UNCHECKED_OPTION_CHAR)
            .required(false)
            .hasArg(false)
            .desc("remove directories without checking UFS contents are in sync")
            .build();

  /**
   * @param fs the filesystem of Alluxio
   */
  public DeleteAttributeCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "deleteAttribute";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public Options getOptions() {
    Option keyOption =
        Option.builder("key").required(true).numberOfArgs(1).desc("key of attributes").build();
    Option valueOption =
        Option.builder("value").required(true).numberOfArgs(1).desc("value of attributes").build();
    Options tmpOptions = new Options().addOption(keyOption);
    tmpOptions.addOption(valueOption);
    return tmpOptions;
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    // TODO(calvin): Remove explicit state checking.
    boolean recursive = cl.hasOption("R");
    boolean numeric = false;
    String mkey = "";
    String mvalue = "";
    double t2value = -1;
    int i;
    if (cl.hasOption("key")) {
      mkey = cl.getOptionValue("key");
    }
    if (cl.hasOption("value")) {
      mvalue = cl.getOptionValue("value");
      if (isNumeric(mvalue)) {
        numeric = true;
        t2value = Double.valueOf(mvalue.toString());
      }
    } else {
      throw new IOException("Delete value! Usage: setAttribute [-R] <path> -key key -value value");
    }
    String[] keylist = mkey.split(":");
    String[] valuelist = mvalue.split(":");
    if (!mFileSystem.exists(path)) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    if (!recursive && mFileSystem.getStatus(path).isFolder()) {
      System.out.println(path + " is a directory, delete attribute to directory only now!");
    }
    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.deleteUDMPath(path.toString());
    for (i = 0; i < keylist.length; i++) {
      options.deleteDUM(keylist[i], valuelist[i]);
      System.out.println("Attributes " + i + " : [key: " + keylist[i] + ", value: " + valuelist[i]
          + "]");
    }
    mFileSystem.setAttribute(path, options);
    System.out.println(" Delete attributes of : " + path + " sucessed!");
  }

  /**
   * Check value is string.
   * @param str input String
   */
  private boolean isNumeric(String str) {
    int i;
    for (i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getUsage() {
    return "deleteAttribute [-R] <path> -key key -value value";
  }

  @Override
  public String getDescription() {
    return "Delete attribute to the specified file. Specify -R to set attribute recursively.";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length < 1) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM_INSUFFICIENT
          .getMessage(getCommandName(), 1, args.length));
    }
  }
}
