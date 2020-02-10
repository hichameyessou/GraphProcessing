/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.exercise6;

public class Config {

  private Config() {}

  public static String pathToSlashdotZoo() {
    return ".\\input\\out.matrix";
  }

  public static String pathToAllVertices() {
    return ".\\input\\all_vertices.csv";
  }

  public static String outputPath() {
    return ".\\output\\";
  }

  public static long randomSeed() {
    return 0xdeadbeef;
  }
}
