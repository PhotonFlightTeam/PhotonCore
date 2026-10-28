package org.photonflight.core;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.photonflight.core.model.Annotation;
import org.photonflight.core.service.PhotonService;

import java.util.List;

@Data
@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class PhotonCore implements PhotonService {

    private final List<Annotation> annotations = Lists.newArrayList();

}
