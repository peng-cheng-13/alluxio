#include <stdio.h>
#include "gethost.h"
#include <jni.h>
#include <stdlib.h>
#include <assert.h>

char* gethost_c(char* argv){
    JavaVMOption options[3];
    JNIEnv *env;
    static JavaVM *jvm=NULL;
    JavaVMInitArgs vm_args;
    long status;
    jclass cls; 
    jmethodID mid;
    jint square;
    jobjectArray applicationArgs;
    jstring appArg;
    jstring msg;
    char* test;
    
    options[0].optionString = "-Djava.class.path=/home/condor/alluxio/core/client/runtime/target/alluxio-core-client-runtime-1.6.1-jar-with-dependencies.jar:.";
    options[1].optionString = "-Xmx256m";
    options[2].optionString = "-Xms128m";
    vm_args.version = 0x00010002;
    vm_args.nOptions = 1; 
    vm_args.options = options;
    if(jvm == NULL){
        status = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args); 
    }else{
	status = jvm->AttachCurrentThread((void**)&env, NULL);
    }
    if(status != JNI_ERR){
	//printf("create JVM successfully\n");
	cls = env->FindClass("GetDataHost");
	if(cls !=0){
	    //printf("find class successfully\n");

	    //get the jmethodID	    
	    mid=env->GetStaticMethodID(cls, "gethost", "(Ljava/lang/String;)Ljava/lang/String;");
	    if(mid !=0){
		 //printf("get static int method successfully\n");

		    appArg = env->NewStringUTF(argv);
		    msg = (jstring)env->CallObjectMethod(cls, mid, appArg);
		    test = (char*)env->GetStringUTFChars(msg, NULL);
		    printf("return string >%s<\n",test);
		    
	    }
	}
	return test;
    }else{
	return NULL;
    }

}

