package net.java21.data2flow.pipeline;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import net.java21.data2flow.contracts.test.arch.Data2flowArchRules;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * design/testing/backend.md §6 공통 규칙(IAM-01.01·NFR-05.01 조직 조건, Thread.sleep·시스템 시계 금지)과 pipeline 고유 규칙:
 * 다른 서비스 스키마에 쓰지 않음, GraalJS는 polyglot API로만(호스트 Truffle 내부 직접 사용 금지), controller → service → repository.
 */
@AnalyzeClasses(packages = "net.java21.data2flow.pipeline", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule organizationScoped = Data2flowArchRules.REPOSITORY_QUERIES_ARE_ORGANIZATION_SCOPED;
    @ArchTest
    static final ArchRule noUnscopedCrud = Data2flowArchRules.UNSCOPED_CRUD_LOOKUPS_ARE_NOT_CALLED;
    @ArchTest
    static final ArchRule noSleep = Data2flowArchRules.NO_THREAD_SLEEP;
    @ArchTest
    static final ArchRule noSystemClock = Data2flowArchRules.NO_SYSTEM_CLOCK;

    /** 샌드박스는 공개 polyglot API만 쓴다(ADR-008). Truffle·GraalJS 내부 클래스에 기대지 않는다 */
    @ArchTest
    static final ArchRule polyglotApiOnly = noClasses().should().dependOnClassesThat()
            .resideInAnyPackage("com.oracle.truffle..", "com.oracle.js..");

    @ArchTest
    static final ArchRule repositoriesDoNotUseServices = noClasses().that().resideInAPackage("..repository..")
            .should().dependOnClassesThat().resideInAnyPackage("..service..", "..controller..");

    @ArchTest
    static final ArchRule servicesDoNotUseControllers = noClasses().that().resideInAPackage("..service..")
            .should().dependOnClassesThat().resideInAPackage("..controller..");
}
