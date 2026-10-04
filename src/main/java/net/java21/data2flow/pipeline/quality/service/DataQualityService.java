package net.java21.data2flow.pipeline.quality.service;

import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.quality.domain.DataQualityScore;
import net.java21.data2flow.pipeline.quality.repository.DataQualityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * 기기별 일일 품질 점수 확정(ING-06.01). 30분마다 돌면서 사이트 시간대(기기 {@code device_state.timezone}, 없으면 조직 기본)로
 * 00:30이 지난 기기의 전날 점수를 한 번 확정한다. 이미 확정한 날은 다시 쓰지 않으므로 그 뒤에 늦게 온 데이터는 반영되지 않는다
 * (TC-ING-070). 예상 주기는 core 기기 정보(기기 → 모델, BR-DEV-08)이고 모르면 완전성을 뺀다.
 */
public class DataQualityService {

    private static final Logger log = LoggerFactory.getLogger(DataQualityService.class);
    private static final LocalTime CONFIRM_AT = LocalTime.of(0, 30);

    private final DataQualityRepository repository;
    private final DeviceDirectory devices;
    private final ZoneId defaultZone;
    private final Clock clock;

    public DataQualityService(DataQualityRepository repository, DeviceDirectory devices, ZoneId defaultZone, Clock clock) {
        this.repository = repository;
        this.devices = devices;
        this.defaultZone = defaultZone;
        this.clock = clock;
    }

    /** 확정할 차례인 기기의 전날 점수를 낸다. 새로 확정한 수 */
    public int confirmDue() {
        Instant now = clock.instant();
        int confirmed = 0;
        for (DataQualityRepository.DeviceDay d : repository.listDevices()) {
            ZoneId zone = zone(d.timezone());
            ZonedDateTime local = now.atZone(zone);
            LocalDate day = local.toLocalDate().minusDays(local.toLocalTime().isBefore(CONFIRM_AT) ? 2 : 1);
            try {
                if (confirm(d.organizationId(), d.deviceId(), day, zone, now)) {
                    confirmed++;
                }
            } catch (RuntimeException e) {
                log.warn("품질 점수 확정 실패(device={}, day={}): {}", d.deviceId(), day, e.getMessage());
            }
        }
        return confirmed;
    }

    /** 한 기기·하루 점수. 이미 있으면 false */
    public boolean confirm(long organizationId, long deviceId, LocalDate day, ZoneId zone, Instant now) {
        if (repository.exists(organizationId, deviceId, day)) {
            return false;
        }
        Instant from = day.atStartOfDay(zone).toInstant();
        Instant to = day.plusDays(1).atStartOfDay(zone).toInstant();
        DataQualityRepository.Counts c = repository.countDay(organizationId, deviceId, from, to);
        Integer interval = intervalOf(deviceId);
        DataQualityScore score = DataQualityScore.calculate(Duration.between(from, to).toSeconds(), interval, c.received(),
                c.late(), c.values(), c.outOfRange(), c.suspect());
        return repository.insert(organizationId, deviceId, day, score, now);
    }

    private Integer intervalOf(long deviceId) {
        Optional<DeviceInfo> info = devices.byId(deviceId);
        if (info.isEmpty()) {
            return null;
        }
        DeviceInfo d = info.get();
        if (d.expectedIntervalSec() != null && d.expectedIntervalSec() > 0) {
            return d.expectedIntervalSec();
        }
        return d.modelIntervalSec() != null && d.modelIntervalSec() > 0 ? d.modelIntervalSec() : null;
    }

    private ZoneId zone(String id) {
        if (id != null && ZoneId.getAvailableZoneIds().contains(id)) {
            return ZoneId.of(id);
        }
        return defaultZone;
    }
}
