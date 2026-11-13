package com.bank.txn.producer;

import com.bank.txn.producer.api.GeneratorController;
import com.bank.txn.producer.generator.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(GeneratorController.class)
class GeneratorControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean GeneratorService service;

    @Test
    void returnsStatus() throws Exception {
        when(service.isRunning()).thenReturn(true);
        when(service.settings()).thenReturn(GeneratorSettings.defaults());
        when(service.stats()).thenReturn(new GeneratorStats(10, 9, 9, 0, 1, 1, 0, 0, 0, 0));
        mvc.perform(get("/api/generator"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running").value(true))
                .andExpect(jsonPath("$.settings.ratePerSecond").value(50))
                .andExpect(jsonPath("$.stats.generated").value(10));
    }

    @Test
    void startAndStop() throws Exception {
        when(service.settings()).thenReturn(GeneratorSettings.defaults());
        when(service.stats()).thenReturn(new GeneratorStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        mvc.perform(post("/api/generator/start")).andExpect(status().isOk());
        mvc.perform(post("/api/generator/stop")).andExpect(status().isOk());
        verify(service).start();
        verify(service).stop();
    }

    @Test
    void rejectsInvalidSettings() throws Exception {
        mvc.perform(put("/api/generator/settings").contentType(MediaType.APPLICATION_JSON).content("""
                        {"ratePerSecond":99999,"accounts":10,"lateRatio":0.9,"maxLatenessSeconds":90,
                         "outOfOrderRatio":0,"duplicateRatio":0,"highValueRatio":0,"burstChancePerSecond":0,"burstSize":12}"""))
                .andExpect(status().isBadRequest());
        verify(service, never()).update(any());
    }

    @Test
    void updatesValidSettings() throws Exception {
        when(service.settings()).thenReturn(GeneratorSettings.defaults());
        when(service.stats()).thenReturn(new GeneratorStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        mvc.perform(put("/api/generator/settings").contentType(MediaType.APPLICATION_JSON).content("""
                        {"ratePerSecond":500,"accounts":10,"lateRatio":0.1,"maxLatenessSeconds":90,
                         "outOfOrderRatio":0,"duplicateRatio":0,"highValueRatio":0,"burstChancePerSecond":0,"burstSize":12}"""))
                .andExpect(status().isOk());
        verify(service).update(new GeneratorSettings(500, 10, 0.1, 90, 0, 0, 0, 0, 12));
    }
}
