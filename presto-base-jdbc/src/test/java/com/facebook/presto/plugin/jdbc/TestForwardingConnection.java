/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc;

import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestForwardingConnection
{
    @Test
    public void testEveryMethodIsForwarded()
    {
        List<String> missing = new ArrayList<>();
        for (Method method : Connection.class.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            try {
                ForwardingConnection.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
            }
            catch (NoSuchMethodException e) {
                missing.add(method.toString());
            }
        }
        assertEquals(missing, List.of());
    }
}
