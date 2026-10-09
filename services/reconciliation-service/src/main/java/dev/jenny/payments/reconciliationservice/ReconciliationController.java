package dev.jenny.payments.reconciliationservice;

import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/reconciliation")
class ReconciliationController {

    private final ReconciliationService service;

    ReconciliationController(ReconciliationService service) {
        this.service = service;
    }

    /** Run now (the scheduler also runs it periodically). */
    @PostMapping("/run")
    ReconciliationService.RunResult run() {
        return service.run();
    }

    @GetMapping("/discrepancies")
    List<Map<String, Object>> discrepancies(@RequestParam(defaultValue = "OPEN") String status) {
        return service.discrepancies(status.toUpperCase());
    }
}
