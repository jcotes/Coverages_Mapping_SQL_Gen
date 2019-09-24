class QueryBuilder:

    @staticmethod
    def build_sql_script(coverages_object, causes, audit_id):
        col_variable_section, col_select_section = QueryBuilder().build_col_variables_txt(causes)
        sql_out_txt = "SET SERVEROUTPUT ON;\n\n"
        sql_out_txt += "DECLARE\n"
        sql_out_txt += QueryBuilder().get_helper_variables_txt()
        sql_out_txt += col_variable_section
        sql_out_txt += "\tBEGIN\n\n"
        sql_out_txt += col_select_section
        sql_out_txt += QueryBuilder().build_coverage_remap_sections(coverages_object, audit_id)
        sql_out_txt += "\n\t\tDBMS_OUTPUT.PUT_LINE('ROWS ADDED: ' || v_ADDED);\n"
        sql_out_txt += "\t\tDBMS_OUTPUT.PUT_LINE('ROWS MODIFIED: ' || v_MODIFIED);\n"
        sql_out_txt += "EXCEPTION\n"
        sql_out_txt += "\tWHEN OTHERS THEN\n"
        sql_out_txt += "\t\tDBMS_OUTPUT.PUT_LINE('Error : ' || sqlerrm);\n"
        sql_out_txt += "END;\n"
        return sql_out_txt


    @staticmethod
    def get_helper_variables_txt():
        helper_vars_txt = "\n\t-- Set up helper variables\n"
        helper_vars_txt += "\tv_CPCL_ROW CIGADMIN.CMS_PC_COV_LINK%ROWTYPE;\n"
        helper_vars_txt += "\tv_EXISTS NUMBER := 0;\n"
        helper_vars_txt += "\tv_COV_ROW CIGADMIN.COVERAGE%ROWTYPE;\n"
        helper_vars_txt += "\tv_COV_IDX CIGADMIN.COVERAGE.COVERAGE%TYPE;\n\n"
        return helper_vars_txt

    @staticmethod
    def build_col_variables_txt(causes):
        col_variable_section = '\t\t-- Declare the variables for cause of loss indices\n'
        col_select_section = '\t\t-- Define the cause of loss variables\n'

        for col in causes:
            cause_of_loss_var = col.upper().replace(',', '').replace(" - ", ' ').replace('(', '')\
                .replace(')', '').replace('-', '').replace(' ', '_')
            col_variable_section += "\tv_{0:35}".format(cause_of_loss_var) + "CIGADMIN.CAUSE_OF_LOSS.CAUSE_OF_LOSS%TYPE;\n"
            col_select_section += "\t\tSELECT CAUSE_OF_LOSS INTO v_" + cause_of_loss_var + " FROM CIGADMIN.CAUSE_OF_LOSS WHERE CAUSE_NAME " \
                                                                                                     "= '" + col + '\';\n'
        col_variable_section += "\n"
        col_select_section += "\n"
        return col_variable_section, col_select_section



    @staticmethod
    def build_coverage_remap_sections(coverages_object, audit_id):
        remap_sections = ""
        for pc_coverage_code in coverages_object:
            for coverage in coverages_object[pc_coverage_code]:
                remap_sections += "\t\t-------------------------------------- BEGIN NEW PC_COVERAGE_CODE --------------------------------------\n" \
                                  "\t\t------- Get the CMS_PC_COV_LINK row for {} -------\n".format(pc_coverage_code)
                remap_sections += "\t\tSELECT *\n" \
                                  "\t\tINTO v_CPCL_ROW\n" \
                                  "\t\tFROM CIGADMIN.CMS_PC_COV_LINK\n" \
                                  "\t\tWHERE PC_COVERAGE_CODE = '{}';\n\n".format(pc_coverage_code)

                remap_sections += "\t\t------- Get the COVERAGE row mapped from {} -------\n".format(pc_coverage_code)
                remap_sections += "\t\tSELECT *\n" \
                                  "\t\tINTO v_COV_ROW\n" \
                                  "\t\tFROM CIGADMIN.COVERAGE\n" \
                                  "\t\tWHERE v_CPCL_ROW.COVERAGE = '{}';\n\n".format(pc_coverage_code)

                for child_coverage in coverage.child_coverages:
                    remap_sections += "\t\t------- Insert new COVERAGE '{}' USING VALUES FROM COVERAGE '{}' -------\n"\
                        .format(child_coverage.coverage_desc, coverage.coverage_desc)
                    remap_sections += QueryBuilder().insert_coverage(child_coverage.coverage_desc, audit_id)

                   # for cause in child_coverage.causes:


        return remap_sections

    @staticmethod
    def insert_coverage(child_coverage_desc, audit_id):
        coverage_insert = "\t\t-- Check if COVERAGE already exists --\n"
        coverage_insert += "\t\tSELECT COUNT(*) INTO v_EXISTS FROM CIGADMIN.COVERAGE\n" \
                           "\t\tWHERE COVERAGE_DESC = '{}'\n" \
                           "\t\tAND AUDIT_ID = '{}';\n\n"

        coverage_insert += "\t\t--- Insert the new coverage --\n"
        coverage_insert += "\t\tINSERT INTO CIGADMIN.COVERAGE (COVERAGE, A_S_COVERAGE_LINE, CLASS, COVERAGE_DESC, CREATE_ID,\n" \
                                      "\t\t\tFIRST_MODIFIED, AUDIT_ID, LAST_MODIFIED, QUICK_CLAIMS_VALID)\n"
        coverage_insert += "\t\tVALUES (\n" \
                          "\t\tv_COV_IDX,\n" \
                          "\t\tv_COV_ROW.A_S_COVERAGE_LINE,\n" \
                          "\t\tv_COV_ROW.CLASS,\n" \
                          "\t\t'{}',\n" \
                          "\t\t'CIGADMIN',\n" \
                          "\t\tSYSDATE,\n" \
                          "\t\t'{}',\n" \
                          "\t\tSYSDATE,\n" \
                          "\t\tv_COV_ROW.QUICK_CLAIMS_VALID);\n\n"\
            .format(child_coverage_desc, audit_id)

        return coverage_insert

    @staticmethod
    def insert_cclink(coverage_idx_var, cause_idx_var, audit_id):
        return  "\n\t\t-- Check if CIGADMIN.CC_LINK exists for the COVERAGE -> CAUSE_OF_LOSS\n" \
                "\t\tSELECT COUNT(*) INTO v_EXISTS FROM CIGADMIN.CC_LINK\n" \
                "\t\tWHERE COVERAGE = {}\n" \
                "\t\tAND CAUSE_OF_LOSS = {}\n" \
                "\t\tAND AUDIT_ID = '{}';\n" \
                "\t\t-- If CC_LINK doesn't exist for COVERAGE -> CAUSE_OF_LOSS, insert into CIGADMIN.CC_LINK\n" \
                "\t\tIF (v_EXISTS = 0) THEN\n" \
                "\t\tINSERT INTO CIGADMIN.CC_LINK (CC_LINK, CAUSE_OF_LOSS, COVERAGE, CREATE_ID, FIRST_MODIFIED, AUDIT_ID, LAST_MODIFIED, EFFECTIVE_DATE, DEPRECATED_DATE)\n" \
                "\t\tVALUES (CIGADMIN.SEQ_CC_LINK.nextval, \n" \
                "\t\t{},\n" \
                "\t\t{},\n" \
                "\t\t'CIGADMIN',\n" \
                "\t\tSYSDATE,\n" \
                "\t\t'CMS-11860',\n" \
                "\t\tSYSDATE,\n" \
                "\t\tSYSDATE,\n" \
                "\t\t'1-JAN-9999'\n\t\t);\n" \
                "\t\tv_CCLINK_INSERTED := v_CCLINK_INSERTED + SQL%ROWCOUNT;\n" \
                "\t\tEND IF;\n".format(coverage_idx_var, cause_idx_var, audit_id, cause_idx_var, coverage_idx_var)

