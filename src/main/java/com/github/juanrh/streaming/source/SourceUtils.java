package com.github.juanrh.streaming.source;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class SourceUtils {
    /**
     * @return the type information extracted first from typeInfo
     * and otherwise from someElement, or throws a RuntimeException
     * in case the information cannot be obtained
     * */
    public static <T> TypeInformation<T> getTypeInformation(Optional<TypeInformation<T>> typeInfo,
                                                            final Supplier<T> someElement) {
        return typeInfo.or(new Supplier<TypeInformation<T>>() {
            @Override
            public TypeInformation<T> get() {
                try {
                    return TypeExtractor.getForObject(someElement.get());
                } catch (Exception e) {
                    throw new RuntimeException("Could not create TypeInformation for type "
                            + someElement.getClass().getName() + " " + e.getMessage() + " "
                            + "; please specify the TypeInformation manually via "
                            + "ElementsWithGapsSource#addElem(TypeInformation, T) or "
                            + "ElementsWithGapsSource#addGap(TypeInformation, Time)");
                }
            }
        });
    }

    public static <T> TypeInformation<T> getTypeInformation(Optional<TypeInformation<T>> typeInfo,
                                                            final T someElement) {
        return getTypeInformation(typeInfo, new Supplier<T>() {
            @Override
            public T get() {
                return someElement;
            }
        });
    }
}
