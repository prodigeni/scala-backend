/**
 *  @author Piotr Molski (moli) <moli@wikia-inc.com>
 */
package com.wikia

import java.util.LinkedHashMap

/**
 * Created with IntelliJ IDEA.
 * User: moli
 * Date: 03.01.13
 */
package object Types {
  type HashMapStr[T] = LinkedHashMap[ String,T ]
  type HashMapStrHash[T] = LinkedHashMap[ String, LinkedHashMap[String, T] ]
  type HashMapStrHashHash[T] = LinkedHashMap[ String, LinkedHashMap[String, LinkedHashMap[String, T]] ]
}
