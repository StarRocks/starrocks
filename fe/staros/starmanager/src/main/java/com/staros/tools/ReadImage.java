// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.staros.tools;

import com.staros.manager.StarManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.lang.System.exit;

/**
 * Read from image and dump to human readable format
 *
 * [Usage] execute the following command under `starmanager/target`,
 *   java -cp ./lib:starmanager.jar com.staros.tools.ReadImage <starmgr_img_path>
 */
public class ReadImage {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("readImage tool requires one parameter: the image filepath");
            exit(1);
        }
        try (InputStream in = Files.newInputStream(Paths.get(args[0]))) {
            StarManager starManager = new StarManager();
            starManager.loadMeta(in);
            starManager.dump();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
