-- ENE-02.02: 가상 센서 계산값(API-ING-25 derived-points)은 공간 가상 기기의 telemetry에 flags 16으로 저장한다
COMMENT ON COLUMN data2flow_pipeline.telemetry.flags IS '비트: 1 late, 2 imported, 4 reprocessed, 8 state_change, 16 derived(가상 센서 계산값, ENE-02.02)';
