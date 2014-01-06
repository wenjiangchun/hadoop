package com.xinyuan.hadoop.exception;

/**
* 未定义异常信息 在Hadoop程序中当Hadoop所需变量没有定义时抛出该异常信息
* @author sofar
*
*/
public class UndefinedException extends Exception {

        private static final long serialVersionUID = 1L;

        public UndefinedException() {
                
        }

        public UndefinedException(String message) {
                super(message);
        }
}