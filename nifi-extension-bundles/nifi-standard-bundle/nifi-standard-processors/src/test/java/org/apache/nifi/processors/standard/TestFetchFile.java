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

package org.apache.nifi.processors.standard;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFetchFile {

    @BeforeEach
    public void prepDestDirectory() throws IOException {
        final Path targetDir = Paths.get("target", "move-target");
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
            return;
        }

        targetDir.toFile().setReadable(true);

        try (Stream<Path> paths = Files.walk(targetDir)) {
            paths.map(Path::toFile).forEach(File::delete);
        }
    }

    @Test
    public void notFound() {
        final Path sourceFile = Paths.get("notFound");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_NONE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_NOT_FOUND, 1);
    }

    @Test
    public void testSimpleSuccess() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_NONE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).getFirst().assertContentEquals(content);

        assertTrue(Files.exists(sourceFile));
    }

    @Test
    public void testDeleteOnComplete() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_DELETE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).getFirst().assertContentEquals(content);

        assertFalse(Files.exists(sourceFile));
    }

    @Test
    public void testMoveOnCompleteWithTargetDirExisting() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);
        final Path destDir = Paths.get("target","move-target");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.assertValid();


        Files.createDirectories(destDir);
        assertTrue(Files.exists(destDir));

        final Path destFile = destDir.resolve(sourceFile.getFileName());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).getFirst().assertContentEquals(content);

        assertFalse(Files.exists(sourceFile));
        assertTrue(Files.exists(destFile));
    }

    @Test
    public void testMoveOnCompleteWithTargetDirMissing() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);

        final Path destDir = Paths.get("target", "move-target");
        if (Files.exists(destDir)) {
            Files.delete(destDir);
        }
        assertFalse(Files.exists(destDir));

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.assertValid();

        final Path destFile = destDir.resolve(sourceFile.getFileName());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).getFirst().assertContentEquals(content);

        assertFalse(Files.exists(sourceFile));
        assertTrue(Files.exists(destFile));
    }

    @Test
    public void testMoveOnCompleteWithTargetExistsButNotWritable() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);

        final Path destDir = Paths.get("target", "move-target");
        if (!Files.exists(destDir)) {
            Files.createDirectories(destDir);
        }
        destDir.toFile().setWritable(false);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.assertValid();

        assertTrue(Files.exists(destDir));
        assertFalse(Files.isWritable(destDir));

        final Path destFile = destDir.resolve(sourceFile.getFileName());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_FAILURE).getFirst().assertContentEquals("");

        assertTrue(Files.exists(sourceFile));
        assertFalse(Files.exists(destFile));
    }

    @Test
    public void testMoveOnCompleteWithParentOfTargetDirNotAccessible() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);

        final Path moveTargetParent = Paths.get("target", "fetch-file");
        final Path moveTarget = moveTargetParent.resolve("move-target");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, moveTarget.toString());
        runner.assertValid();

        // Make the parent of move-target non-writable and non-readable
        Files.createDirectories(moveTargetParent);
        moveTargetParent.toFile().setReadable(false);
        moveTargetParent.toFile().setWritable(false);

        try {
            runner.enqueue(new byte[0]);
            runner.run();
            runner.assertAllFlowFilesTransferred(FetchFile.REL_FAILURE, 1);
            runner.getFlowFilesForRelationship(FetchFile.REL_FAILURE).getFirst().assertContentEquals("");

            assertTrue(Files.exists(sourceFile));
        } finally {
            moveTargetParent.toFile().setReadable(true);
            moveTargetParent.toFile().setWritable(true);
        }
    }

    @Test
    public void testMoveAndReplace() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);
        final Path destDir = Paths.get("target", "move-target");
        Files.createDirectories(destDir);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_REPLACE.getValue());
        runner.assertValid();


        final Path destFile = destDir.resolve(sourceFile.getFileName());
        Files.write(destFile, "Good-bye".getBytes(), StandardOpenOption.CREATE);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).getFirst().assertContentEquals(content);

        final byte[] replacedContent = Files.readAllBytes(destFile);
        assertArrayEquals(content, replacedContent);
        assertFalse(Files.exists(sourceFile));
        assertTrue(Files.exists(destFile));
    }

    @Test
    public void testMoveAndKeep() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);
        final Path destDir = Paths.get("target","move-target");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_KEEP_INTACT.getValue());
        runner.assertValid();

        final Path destFile = destDir.resolve(sourceFile.getFileName());

        final byte[] goodBye = "Good-bye".getBytes();
        Files.write(destFile, goodBye);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).getFirst().assertContentEquals(content);

        final byte[] replacedContent = Files.readAllBytes(destFile);
        assertArrayEquals(goodBye, replacedContent);
        assertFalse(Files.exists(sourceFile));
        assertTrue(Files.exists(destFile));
    }

    @Test
    public void testMoveAndFail() throws IOException {
        final Path sourceFile = Paths.get("target","1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);
        final Path destDir = Paths.get("target","move-target");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_FAIL.getValue());
        runner.assertValid();


        final Path destFile = destDir.resolve(sourceFile.getFileName());

        final byte[] goodBye = "Good-bye".getBytes();
        Files.write(destFile, goodBye);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_FAILURE, 1);

        final byte[] replacedContent = Files.readAllBytes(destFile);
        assertArrayEquals(goodBye, replacedContent);
        assertTrue(Files.exists(sourceFile));
        assertTrue(Files.exists(destFile));
    }


    @Test
    public void testMoveAndRename() throws IOException {
        final Path sourceFile = Paths.get("target", "1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile, content, StandardOpenOption.CREATE);
        final Path destDir = Paths.get("target", "move-target");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.toString());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, destDir.toString());
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_RENAME.getValue());
        runner.assertValid();

        final Path destFile = destDir.resolve(sourceFile.getFileName());

        final byte[] goodBye = "Good-bye".getBytes();
        Files.write(destFile, goodBye);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);

        final byte[] replacedContent = Files.readAllBytes(destFile);
        assertArrayEquals(goodBye, replacedContent);
        assertFalse(Files.exists(sourceFile));
        assertTrue(Files.exists(destFile));

        assertEquals(2, destDir.toFile().list().length);
    }
}
