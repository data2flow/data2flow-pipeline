package net.java21.data2flow.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR-008·018 라이선스 정책: GraalJS는 커뮤니티판(polyglot + js-community, UPL)만. Oracle판 {@code org.graalvm.polyglot:js}(GFTC)·
 * {@code truffle-enterprise}·isolate 런타임이 클래스패스에 없어야 한다(빌드의 maven-enforcer bannedDependencies와 이중 확인).
 */
class LicenseGuardTest {

    @Test
    @DisplayName("[NFR-03.04][ADR-008] 클래스패스에 GraalJS 커뮤니티판만 있다")
    void communityEditionOnly() {
        List<String> entries = Arrays.asList(System.getProperty("java.class.path").split(File.pathSeparator));

        assertThat(entries).anyMatch(e -> e.contains("/org/graalvm/js/js-language/"));
        assertThat(entries).anyMatch(e -> e.contains("/org/graalvm/polyglot/polyglot/"));
        assertThat(entries).noneMatch(e -> e.contains("/org/graalvm/polyglot/js/"));
        assertThat(entries).noneMatch(e -> e.contains("truffle-enterprise") || e.contains("-isolate"));
    }
}
