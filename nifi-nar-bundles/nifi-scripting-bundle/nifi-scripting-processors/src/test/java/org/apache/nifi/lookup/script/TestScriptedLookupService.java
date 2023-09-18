/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.lookup.script;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the ScriptedLookupService controller service
 */
public class TestScriptedLookupService {
    @TempDir
    private static Path targetPath;
    @TempDir
    private static Path alternateTargetPath;
    private ScriptedLookupService scriptedLookupService;
    private TestRunner runner;

    @BeforeAll
    public static void setUpOnce() throws Exception {
        Files.copy(Paths.get("src/test/resources/groovy/test_lookup_inline.groovy"), targetPath, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get("src/test/resources/groovy/test_simple_lookup_inline.groovy"), alternateTargetPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @BeforeEach
    public void setUp() throws Exception {
        scriptedLookupService = new MockScriptedLookupService();
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });
        runner.addControllerService("lookupService", scriptedLookupService);
        runner.setProperty(scriptedLookupService, "Script Engine", "Groovy");
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(scriptedLookupService);
    }

    @Test
    void testLookupServiceGroovyScript() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>(1);
        map.put("key", "Hello");
        Optional<Object> opt = scriptedLookupService.lookup(map);
        assertTrue(opt.isPresent());
        assertEquals("Hi", opt.get());
        map = new LinkedHashMap<>(1);
        map.put("key", "World");
        opt = scriptedLookupService.lookup(map);
        assertTrue(opt.isPresent());
        assertEquals("there", opt.get());
        map = new LinkedHashMap<>(1);
        map.put("key", "Not There");
        opt = scriptedLookupService.lookup(map);
        assertFalse(opt.isPresent());
    }

    @Test
    void testLookupServiceScriptReload() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>(1);
        map.put("key", "Hello");
        Optional<Object> opt = scriptedLookupService.lookup(map);
        assertTrue(opt.isPresent());
        assertEquals("Hi", opt.get());
        map = new LinkedHashMap<>(1);
        map.put("key", "World");
        opt = scriptedLookupService.lookup(map);
        assertTrue(opt.isPresent());
        assertEquals("there", opt.get());
        map = new LinkedHashMap<>(1);
        map.put("key", "Not There");
        opt = scriptedLookupService.lookup(map);
        assertFalse(opt.isPresent());

        // Disable and load different script
        runner.disableControllerService(scriptedLookupService);
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.SCRIPT_FILE, alternateTargetPath.toString());
        runner.enableControllerService(scriptedLookupService);

        map = new LinkedHashMap<>(1);
        map.put("key", "Hello");
        opt = scriptedLookupService.lookup(map);
        assertTrue(opt.isPresent());
        assertEquals("Goodbye", opt.get());
        map = new LinkedHashMap<>(1);
        map.put("key", "World");
        opt = scriptedLookupService.lookup(map);
        assertTrue(opt.isPresent());
        assertEquals("Stranger", opt.get());
        map = new LinkedHashMap<>(1);
        map.put("key", "Not There");
        opt = scriptedLookupService.lookup(map);
        assertFalse(opt.isPresent());
    }

    public static class MockScriptedLookupService extends ScriptedLookupService implements AccessibleScriptingComponentHelper {
        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}
