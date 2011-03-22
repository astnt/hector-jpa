/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors : Pedro Gomes and Universidade do Minho.
    		 : Todd Nine
 ***********************************************************************/
package com.datastax.hectorjpa.spring;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.beanutils.MethodUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.datastax.hectorjpa.consitency.JPAConsistency;

/**
 * Interceptor aspect to maintain a stack of consistency levels for the current
 * Thread and set the level based on the annotation of the method. A lot of this
 * code is a duplicate of Beanutils. Will remain a copy until this bug is fixed.
 * 
 * https://issues.apache.org/jira/browse/BEANUTILS-381
 * 
 * 
 * @author Todd Nine
 * 
 */
@Aspect
@Component
public class ConsistencyLevelAspect {

	private static final Logger logger = LoggerFactory
			.getLogger(ConsistencyLevelAspect.class);

	/**
	 * Cache for each invocation target and it's params to increase speed after
	 * first invocation
	 */
	private static final ConcurrentMap<TargetKey, ConsistencyLevel> consistencyAnnotation = new ConcurrentHashMap<TargetKey, ConsistencyLevel>();

	/**
	 * Cache to check if we have the annotation
	 */
	private static final ConcurrentMap<TargetKey, Boolean> hasAnnotation = new ConcurrentHashMap<TargetKey, Boolean>();

	/**
	 * Our thread local stack for invoking this interceptor
	 */
	private static final ThreadLocal<Stack<ConsistencyLevel>> threadStack = new ThreadLocal<Stack<ConsistencyLevel>>() {

		@Override
		protected Stack<ConsistencyLevel> initialValue() {
			return new Stack<ConsistencyLevel>();
		}

	};

	public ConsistencyLevelAspect() {

	}

	/**
	 * Validates any method that has the valid annotation on it and is wired as
	 * a spring service
	 * 
	 * @param jp
	 * @throws Throwable
	 */
	@Around("@annotation(com.datastax.hectorjpa.spring.Consistency)")
	public Object setConsistency(ProceedingJoinPoint pjp) throws Throwable {

		logger.debug(
				"Invoking before advice for @Consistency annotation.  Target object is {} on method {}",
				pjp.getTarget(), pjp.getSignature());

		MethodSignature sig = (MethodSignature) pjp.getSignature();

		Object[] args = pjp.getArgs();

		Method signatureMethod = sig.getMethod();

		Class<?>[] signatureTypes = signatureMethod.getParameterTypes();

		// we do this because we want to get the best match from the child
		// classes
		Class<?>[] runtimeArgs = new Class<?>[signatureTypes.length];

		for (int i = 0; i < signatureTypes.length; i++) {

			if (args[i] != null) {
				runtimeArgs[i] = args[i].getClass();
			} else {
				runtimeArgs[i] = signatureTypes[i];
			}
		}

		Class<?> runtimeClass = pjp.getTarget().getClass();

		// check if this is annotated, if not proceed and execute it

		ConsistencyLevel level = consistency(runtimeClass,
				signatureMethod.getName(), runtimeArgs);

		if (level == null) {
			return pjp.proceed(args);
		}

		Stack<ConsistencyLevel> stack = threadStack.get();

		stack.push(level);
		JPAConsistency.set(level);

		Object result = null;

		try {
			result = pjp.proceed(args);
		} finally {
			stack.pop();

			if (stack.size() > 0) {
				JPAConsistency.set(stack
						.peek());
			}else{
			  JPAConsistency.remove();
			}

		}

		return result;
	}

	/**
	 * Scans the given method with the given parameter types. If a check
	 * annotation is present, the annotated level will be returned
	 * 
	 * @param target
	 * @param methodName
	 * @param paramTypes
	 * @return
	 */
	private ConsistencyLevel consistency(Class<?> target, String methodName,
			Class<?>[] paramTypes) {

		TargetKey key = new TargetKey(target, methodName, paramTypes);

		Boolean cached = hasAnnotation.get(key);

		if (cached != null) {
			if (cached) {
				return consistencyAnnotation.get(key);
			}
			return null;
		}

		// try a direct method lookup
		Method targetMethod = getMatchingAccessibleMethod(target, methodName,
				paramTypes);

		Annotation[] annotations = targetMethod.getAnnotations();

		for (int i = 0; i < annotations.length; i++) {
			if (annotations[i] instanceof Consistency) {

				ConsistencyLevel level = ((Consistency) annotations[i]).value();

				consistencyAnnotation.putIfAbsent(key, level);
				hasAnnotation.putIfAbsent(key, true);
				return level;
			}
		}

		return null;
	}

	/**
	 * Get the closest match to our method. Will check inheritance as well as
	 * overloading
	 * 
	 * @param target
	 * @param methodName
	 * @param paramTypes
	 * @return
	 */
	private Method getMatchingAccessibleMethod(Class<?> clazz,
			String methodName, Class<?>[] parameterTypes) {

		// search through all methods
		int paramSize = parameterTypes.length;
		Method bestMatch = null;
		Method[] methods = clazz.getMethods();
		float bestMatchCost = Float.MAX_VALUE;
		float myCost = Float.MAX_VALUE;
		for (int i = 0, size = methods.length; i < size; i++) {
			if (methods[i].getName().equals(methodName)) {

				// compare parameters
				Class<?>[] methodsParams = methods[i].getParameterTypes();
				int methodParamSize = methodsParams.length;
				if (methodParamSize == paramSize) {
					boolean match = true;
					for (int n = 0; n < methodParamSize; n++) {

						if (!MethodUtils.isAssignmentCompatible(
								methodsParams[n], parameterTypes[n])) {

							match = false;
							break;
						}
					}

					if (match) {
						// get accessible version of method
						Method method = MethodUtils.getAccessibleMethod(clazz,
								methods[i]);
						if (method != null) {

							myCost = getTotalTransformationCost(parameterTypes,
									method.getParameterTypes());
							if (myCost < bestMatchCost) {
								bestMatch = method;
								bestMatchCost = myCost;
							}
						}

					}
				}
			}
		}

		return bestMatch;
	}

	/**
	 * Returns the sum of the object transformation cost for each class in the
	 * source argument list.
	 * 
	 * @param srcArgs
	 *            The source arguments
	 * @param destArgs
	 *            The destination arguments
	 * @return The total transformation cost
	 */
	private float getTotalTransformationCost(Class<?>[] srcArgs,
			Class<?>[] destArgs) {

		float totalCost = 0.0f;
		for (int i = 0; i < srcArgs.length; i++) {
			Class<?> srcClass, destClass;
			srcClass = srcArgs[i];
			destClass = destArgs[i];
			totalCost += getObjectTransformationCost(srcClass, destClass);
		}

		return totalCost;
	}

	/**
	 * Gets the number of steps required needed to turn the source class into
	 * the destination class. This represents the number of steps in the object
	 * hierarchy graph.
	 * 
	 * @param srcClass
	 *            The source class
	 * @param destClass
	 *            The destination class
	 * @return The cost of transforming an object
	 */
	private float getObjectTransformationCost(Class<?> srcClass,
			Class<?> destClass) {
		float cost = 0.0f;
		while (srcClass != null && !destClass.equals(srcClass)) {
			if (destClass.isInterface()
					&& MethodUtils.isAssignmentCompatible(destClass, srcClass)) {
				// slight penalty for interface match.
				// we still want an exact match to override an interface match,
				// but
				// an interface match should override anything where we have to
				// get a
				// superclass.
				cost += 0.25f;
				break;
			}
			cost++;
			srcClass = srcClass.getSuperclass();
		}

		/*
		 * If the destination class is null, we've travelled all the way up to
		 * an Object match. We'll penalize this by adding 1.5 to the cost.
		 */
		if (srcClass == null) {
			cost += 1.5f;
		}

		return cost;
	}

	private class TargetKey {

		private Class<?> clazz;
		private String methodName;
		private Class<?>[] paramTypes;

		public TargetKey(Class<?> clazz, String methodName,
				Class<?>[] paramTypes) {
			super();
			this.clazz = clazz;
			this.methodName = methodName;
			this.paramTypes = paramTypes;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
			result = prime * result
					+ ((methodName == null) ? 0 : methodName.hashCode());
			result = prime * result + Arrays.hashCode(paramTypes);
			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof TargetKey))
				return false;
			TargetKey other = (TargetKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (clazz == null) {
				if (other.clazz != null)
					return false;
			} else if (!clazz.equals(other.clazz))
				return false;
			if (methodName == null) {
				if (other.methodName != null)
					return false;
			} else if (!methodName.equals(other.methodName))
				return false;
			if (!Arrays.equals(paramTypes, other.paramTypes))
				return false;
			return true;
		}

		private ConsistencyLevelAspect getOuterType() {
			return ConsistencyLevelAspect.this;
		}
	}

}
