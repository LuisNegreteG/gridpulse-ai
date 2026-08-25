# GridPulse AI — Data Contracts

**Project:** GridPulse AI — Ontario Real-Time Energy Intelligence & DataOps Platform  
**Contract version:** 0.1  
**Status:** Draft — validated through source discovery  
**Last reviewed:** 2026-08-25

---

# 1. Purpose

This document defines the initial data contracts between:

1. IESO public source reports;
2. GridPulse Silver datasets;
3. downstream analytical and real-time consumers.

The contracts distinguish three concepts:

```text
SOURCE CONTRACT
What the external source actually publishes

        ↓

SILVER CONTRACT
What GridPulse guarantees after normalization

        ↓

DATA QUALITY RULES
How violations and suspicious conditions are handled
