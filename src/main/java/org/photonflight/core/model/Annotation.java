package org.photonflight.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Annotation {

    private double x, y, z;
    private String text;

}
