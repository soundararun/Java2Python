package com.zif.cep.test;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.runner.RunWith;


/**
 * @author Vijay
 *
 */
@RunWith(JUnitPlatform.class)
//@SelectPackages("com.zif.cep.test")
@SelectClasses({RuleEvaluatorTest.class, DynamicKeyFunctionTest.class, StreamingJobTest.class})
public class AllUnitTests {

}