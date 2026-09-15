package org.photonflight.core;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import org.photonflight.core.service.PhotonService;

import java.util.Set;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class PhotonCore implements PhotonService {

    private final Set<PhotonService> services;

}
