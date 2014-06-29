package cassandra.benchmark.service.internal.helper;

import java.util.List;

public class Transformer {

    public static Long[] transform(final List<Long> list) {
        if(list == null) return null;
        if(list.size() == 0) return new Long[0];

        final Long[] result = new Long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }

        return result;
    }
}
