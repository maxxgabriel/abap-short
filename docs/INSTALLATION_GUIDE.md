# Installation Guide

## Complete Step-by-Step Installation Instructions

### Prerequisites Checklist
- [ ] SAP NetWeaver ABAP 7.40 or higher
- [ ] Developer access (SE11, SE24, SE38, SE80)
- [ ] Development key registered
- [ ] Package creation authorization

---

## Phase 1: Package Setup (5 minutes)

### Step 1.1: Create Package
```
Transaction: SE80
Action: Create Package

Fields:
  Package:     $ZETL
  Description: Sales ETL System
  Package Type: Development
```

**Note**: Use `$` prefix for local packages or `Z` prefix for transportable packages.

---

## Phase 2: Database Objects (10 minutes)

### Step 2.1: Create ZSALES_RAW Table
```
Transaction: SE11
Action: Create Database Table

1. Copy content from: src/tables/ZSALES_RAW.txt
2. Table name: ZSALES_RAW
3. Delivery Class: A - Application table
4. Table Category: TRANSP - Transparent table
5. Save and Activate
```

### Step 2.2: Create ZSALES_ANALYTICS Table
```
Transaction: SE11
Action: Create Database Table

1. Copy content from: src/tables/ZSALES_ANALYTICS.txt
2. Table name: ZSALES_ANALYTICS
3. Delivery Class: A - Application table
4. Table Category: TRANSP - Transparent table
5. Save and Activate
```

### Step 2.3: Create ZETL_LOG Table
```
Transaction: SE11
Action: Create Database Table

1. Copy content from: src/tables/ZETL_LOG.txt
2. Table name: ZETL_LOG
3. Delivery Class: A - Application table
4. Table Category: TRANSP - Transparent table
5. Save and Activate
```

---

## Phase 3: Type Definitions (5 minutes)

### Step 3.1: Create ZETL_TYPES Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZETL_TYPES
2. Description: ETL Type Definitions
3. Class type: Normal class
4. Copy content from: src/types/ZETL_TYPES.abap
5. Save and Activate
```

---

## Phase 4: Interfaces (10 minutes)

### Step 4.1: Create ZIF_ETL_LOGGER Interface
```
Transaction: SE24
Action: Create Interface

1. Interface name: ZIF_ETL_LOGGER
2. Description: ETL Logger Interface
3. Copy content from: src/interfaces/ZIF_ETL_LOGGER.abap
4. Save and Activate
```

### Step 4.2: Create ZIF_ETL_COMPONENT Interface
```
Transaction: SE24
Action: Create Interface

1. Interface name: ZIF_ETL_COMPONENT
2. Description: ETL Component Interface
3. Copy content from: src/interfaces/ZIF_ETL_COMPONENT.abap
4. Save and Activate
```

---

## Phase 5: Exception Classes (5 minutes)

### Step 5.1: Create ZCX_ETL_ERROR Exception
```
Transaction: SE24
Action: Create Class

1. Class name: ZCX_ETL_ERROR
2. Description: ETL Exception Class
3. Exception Class: Check "Exception Class"
4. Superclass: CX_STATIC_CHECK
5. Copy content from: src/exceptions/ZCX_ETL_ERROR.abap
6. Save and Activate
```

---

## Phase 6: Constants and Includes (10 minutes)

### Step 6.1: Create ZCL_ETL_CONSTANTS Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZCL_ETL_CONSTANTS
2. Description: ETL Constants
3. Copy content from: src/classes/ZCL_ETL_CONSTANTS.abap
4. Save and Activate
```

### Step 6.2: Create ZETL_TOP Include
```
Transaction: SE38
Action: Create Include

1. Program name: ZETL_TOP
2. Type: Include
3. Copy content from: src/includes/ZETL_TOP.abap
4. Save and Activate
```

### Step 6.3: Create ZETL_MACROS Include
```
Transaction: SE38
Action: Create Include

1. Program name: ZETL_MACROS
2. Type: Include
3. Copy content from: src/includes/ZETL_MACROS.abap
4. Save and Activate
```

---

## Phase 7: ETL Component Classes (20 minutes)

### Step 7.1: Create ZCL_ETL_LOGGER Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZCL_ETL_LOGGER
2. Description: ETL Logger
3. Interfaces: ZIF_ETL_LOGGER
4. Copy content from: src/classes/ZCL_ETL_LOGGER.abap
5. Save and Activate
```

### Step 7.2: Create ZCL_ETL_EXTRACTOR Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZCL_ETL_EXTRACTOR
2. Description: ETL Data Extractor
3. Copy content from: src/classes/ZCL_ETL_EXTRACTOR.abap
4. Dependencies: ZCL_ETL_LOGGER
5. Save and Activate
```

### Step 7.3: Create ZCL_ETL_TRANSFORMER Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZCL_ETL_TRANSFORMER
2. Description: ETL Data Transformer
3. Copy content from: src/classes/ZCL_ETL_TRANSFORMER.abap
4. Dependencies: ZCL_ETL_LOGGER
5. Save and Activate
```

### Step 7.4: Create ZCL_ETL_LOADER Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZCL_ETL_LOADER
2. Description: ETL Data Loader
3. Copy content from: src/classes/ZCL_ETL_LOADER.abap
4. Dependencies: ZCL_ETL_LOGGER
5. Save and Activate
```

### Step 7.5: Create ZCL_ETL_ORCHESTRATOR Class
```
Transaction: SE24
Action: Create Class

1. Class name: ZCL_ETL_ORCHESTRATOR
2. Description: ETL Orchestrator
3. Copy content from: src/classes/ZCL_ETL_ORCHESTRATOR.abap
4. Dependencies: All above classes
5. Save and Activate
```

---

## Phase 8: Main Program (5 minutes)

### Step 8.1: Create Z_SALES_ETL_MAIN Program
```
Transaction: SE38
Action: Create Program

1. Program name: Z_SALES_ETL_MAIN
2. Type: Executable Program
3. Copy content from: src/programs/Z_SALES_ETL_MAIN.abap
4. Save and Activate
```

---

## Phase 9: Verification (10 minutes)

### Step 9.1: Check All Objects
```
Transaction: SE80
Action: Open Package $ZETL

Verify presence of:
☑ 3 Database Tables
☑ 1 Type Class
☑ 2 Interfaces
☑ 1 Exception Class
☑ 6 ABAP Classes
☑ 2 Include Programs
☑ 1 Executable Program
```

### Step 9.2: Check Dependencies
```
Transaction: SE80
Select: ZCL_ETL_ORCHESTRATOR
Menu: Utilities → Dependency → Dependency Graph

Should show all dependent classes properly linked.
```

---

## Phase 10: Initial Test (5 minutes)

### Step 10.1: Run in Test Mode
```
Transaction: SE38 or SA38
Program: Z_SALES_ETL_MAIN

Selection Screen:
  From Date: [7 days ago]
  To Date: [Today]
  Test Mode: ☑ Checked

Press: Execute (F8)
```

### Expected Output:
```
=== EXTRACT Phase ===
Extracted 5 records successfully

=== TRANSFORM Phase ===
Transformed 5 of 5 records

=== LOAD Phase ===
Loaded 5 of 5 records

*** ETL Process Completed Successfully ***
```

---

## Troubleshooting

### Issue: Syntax Error in Class
**Solution**: Ensure ABAP version is 7.40+
- Check inline declarations (DATA(lv_var))
- Check VALUE constructor (#( ))
- Check string templates (|text|)

### Issue: Interface Not Found
**Solution**: Install interfaces before classes
- ZIF_ETL_LOGGER before ZCL_ETL_LOGGER
- ZIF_ETL_COMPONENT before component classes

### Issue: Type Not Found
**Solution**: Install ZETL_TYPES before other classes

### Issue: Include Not Found
**Solution**: Create includes as separate programs, not in classes

### Issue: Table Does Not Exist
**Solution**:
1. Go to SE11
2. Verify table is activated (not just saved)
3. Generate table maintenance if needed

---

## Post-Installation Configuration

### Configure Business Rules
Edit configuration in ZCL_ETL_CONSTANTS:
- Discount rates and thresholds
- Tax rate
- Category thresholds
- Batch sizes

### Optional: Create Transaction Code
```
Transaction: SE93
Action: Create Transaction

Transaction Code: ZETL
Program: Z_SALES_ETL_MAIN
Screen Number: 1000

Now you can run: Transaction ZETL
```

### Optional: Schedule Background Job
```
Transaction: SM36
Action: Define Background Job

Job Name: ZETL_DAILY
Program: Z_SALES_ETL_MAIN
Frequency: Daily
Time: 02:00 AM
```

---

## Installation Checklist

Use this checklist to track your progress:

**Phase 1: Package**
- [ ] Package $ZETL created

**Phase 2: Database**
- [ ] ZSALES_RAW created and activated
- [ ] ZSALES_ANALYTICS created and activated
- [ ] ZETL_LOG created and activated

**Phase 3: Types**
- [ ] ZETL_TYPES created and activated

**Phase 4: Interfaces**
- [ ] ZIF_ETL_LOGGER created and activated
- [ ] ZIF_ETL_COMPONENT created and activated

**Phase 5: Exceptions**
- [ ] ZCX_ETL_ERROR created and activated

**Phase 6: Constants/Includes**
- [ ] ZCL_ETL_CONSTANTS created and activated
- [ ] ZETL_TOP created and activated
- [ ] ZETL_MACROS created and activated

**Phase 7: Classes**
- [ ] ZCL_ETL_LOGGER created and activated
- [ ] ZCL_ETL_EXTRACTOR created and activated
- [ ] ZCL_ETL_TRANSFORMER created and activated
- [ ] ZCL_ETL_LOADER created and activated
- [ ] ZCL_ETL_ORCHESTRATOR created and activated

**Phase 8: Program**
- [ ] Z_SALES_ETL_MAIN created and activated

**Phase 9: Verification**
- [ ] All objects visible in package
- [ ] Dependency graph shows correct links
- [ ] No syntax errors

**Phase 10: Testing**
- [ ] Test execution successful
- [ ] Sample data processed
- [ ] Logs generated

---

## Time Estimate

| Phase | Duration | Total |
|-------|----------|-------|
| Package Setup | 5 min | 5 min |
| Database Objects | 10 min | 15 min |
| Type Definitions | 5 min | 20 min |
| Interfaces | 10 min | 30 min |
| Exceptions | 5 min | 35 min |
| Constants/Includes | 10 min | 45 min |
| ETL Classes | 20 min | 65 min |
| Main Program | 5 min | 70 min |
| Verification | 10 min | 80 min |
| Testing | 5 min | **85 min** |

**Total Installation Time: ~1.5 hours**

---

## Next Steps

After successful installation:
1. Review configuration in `config/etl_config.json`
2. Read `docs/QUICKSTART.md` for usage examples
3. Review `docs/PROJECT_STRUCTURE.md` for architecture details
4. Add real sales data to ZSALES_RAW
5. Run ETL in production mode (uncheck test mode)

---

## Support

If you encounter issues:
1. Check syntax errors: SE24 → Class → Check
2. Verify dependencies: SE80 → Utilities → Dependencies
3. Check system log: SM21
4. Review ETL logs: SE16N → ZETL_LOG
5. Enable debugging: SE38 → F5 (Debugging)

**Installation Complete! Ready to process sales data.**
