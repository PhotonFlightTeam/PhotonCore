package org.photonflight.core;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.photonflight.core.model.Point;
import org.photonflight.core.service.PhotonService;

import java.util.List;
import java.util.Set;

@Data
@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class PhotonCore implements PhotonService {

    private final Set<PhotonService> services;

    // This may not be what we end up with, reason being
    // that deletions/lookups would not be very quick.
    private final List<Point> points = Lists.newArrayList();

}
