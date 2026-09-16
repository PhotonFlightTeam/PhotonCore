package org.photonflight.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.awt.*;

/// A basic mutable 3D coordinate point
@Data
@AllArgsConstructor
public class Point {

    private double x, y, z;
    private Color color;

}
