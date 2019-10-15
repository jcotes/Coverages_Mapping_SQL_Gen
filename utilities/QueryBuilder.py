class QueryBuilder:

    @staticmethod
    def build_sql_script(coverages_object, causes, audit_id):
        col_variable_section, col_select_section = QueryBuilder().build_col_variables_txt(causes)
        coverages_cursors = QueryBuilder().build_cov_cursors_txt(coverages_object)
        coverage_loops = QueryBuilder().build_cursor_loops(coverages_object, audit_id)
        sql_out_txt = "SET SERVEROUTPUT ON;\n\n"
        sql_out_txt += "DECLARE\n"
        sql_out_txt += QueryBuilder().get_helper_variables_txt()
        sql_out_txt += col_variable_section
        sql_out_txt += coverages_cursors
        sql_out_txt += "\tBEGIN\n\n"
        sql_out_txt += col_select_section
        sql_out_txt += "\n"
        sql_out_txt += coverage_loops
        sql_out_txt += "\n\t\tDBMS_OUTPUT.PUT_LINE('ROWS ADDED (CIGADMIN.COVERAGE): ' || v_COVERAGES_INSERTED);\n"
        sql_out_txt += "\t\tDBMS_OUTPUT.PUT_LINE('ROWS ADDED (CIGADMIN.CC_LINK): ' || v_CCLINK_INSERTED);\n"
        sql_out_txt += "\t\tDBMS_OUTPUT.PUT_LINE('ROWS ADDED (CIGADMIN.CMS_COV_COV_MAP): ' || v_COV_MAP_INSERTED);\n"
        sql_out_txt += "EXCEPTION\n"
        sql_out_txt += "\tWHEN OTHERS THEN\n"
        sql_out_txt += "\t\tDBMS_OUTPUT.PUT_LINE('Error : ' || sqlerrm);\n"
        sql_out_txt += "END;\n"
        return sql_out_txt


    @staticmethod
    def get_helper_variables_txt():
        helper_vars_txt = "\n\t\t-- Set up helper variables\n"
        helper_vars_txt += "\t\tv_OLD_COVERAGE CIGADMIN.COVERAGE%ROWTYPE;\n"
        helper_vars_txt += "\t\tv_EXISTS NUMBER := 0;\n"
        helper_vars_txt += "\t\tv_COVERAGES_INSERTED NUMBER := 0;\n"
        helper_vars_txt += "\t\tv_COV_MAP_INSERTED NUMBER := 0;\n"
        helper_vars_txt += "\t\tv_CCLINK_INSERTED NUMBER := 0;\n"
        helper_vars_txt += "\t\tv_COV_IDX CIGADMIN.COVERAGE.COVERAGE%TYPE := CIGADMIN.SEQ_COVERAGE.nextval;\n"
        helper_vars_txt += "\t\tv_CCLINK_IDX CIGADMIN.CC_LINK.CC_LINK%TYPE := CIGADMIN.SEQ_CC_LINK.nextval;\n\n"
        return helper_vars_txt

    @staticmethod
    def build_col_variables_txt(causes):
        col_variable_section = '\t\t-- Declare the variables for cause of loss indices\n'
        col_select_section = '\t\t-- Define the cause of loss variables\n'
        col_variable_section += ""
        col_select_section += ""

        for col in causes:
            cause_of_loss_var = col.upper().replace(',', '').replace(' ', '_')
            col_variable_section += "\t\tv_{0:35}".format(cause_of_loss_var) + "CIGADMIN.CAUSE_OF_LOSS.CAUSE_OF_LOSS%TYPE;\n"
            col_select_section += "\t\tSELECT CAUSE_OF_LOSS INTO v_" + cause_of_loss_var + " FROM CIGADMIN.CAUSE_OF_LOSS WHERE CAUSE_NAME " \
                                                                                             "= '" + col + '\';\n'
        return col_variable_section, col_select_section


    @staticmethod
    def build_cov_cursors_txt(coverages_object):
        coverages_cursors = ""
        for lob in coverages_object:
            for coverage in coverages_object[lob]:
                coverages_cursors += "\n\t\tCURSOR c_" + coverage.desc.replace(" ", "_").replace("'", "").replace(",", "").replace("\'", "").replace("`", "").upper() + " IS\n"
                coverages_cursors += "\t\tSELECT DISTINCT COV.COVERAGE, COV.A_S_COVERAGE_LINE, COV.CLASS, COV.COVERAGE_DESC, " \
                                     "COV.CREATE_ID, \n\t\t\t\t\tCOV.FIRST_MODIFIED, COV.AUDIT_ID, COV.LAST_MODIFIED, COV.QUICK_CLAIMS_VALID \n" \
                                     "\t\tFROM CIGADMIN.COVERAGE COV\n" \
                                     "\t\tJOIN CIGADMIN.A_S_COVERAGE_LINE ASCL ON ASCL.A_S_COVERAGE_LINE = COV.A_S_COVERAGE_LINE\n" \
                                     "\t\tWHERE COV.CLASS = '{:06d}'\n" \
                                     "\t\tAND ASCL.LINE_NBR = '{}'\n" \
                                     "\t\tAND COV.COVERAGE_DESC = '{}';\n"\
                    .format(int(coverage.clazz), coverage.line, coverage.desc)
        return coverages_cursors

    @staticmethod
    def build_cursor_loops(coverages_object, audit_id):
        coverages_loops = ""
        for lob in coverages_object:
            for coverage in coverages_object[lob]:
                cov_cursor = "c_" + coverage.desc.replace(" ", "_").replace("'", "").replace(",", "").replace("\'", "").replace("`", "").upper()
                coverages_loops += "\n\t\t--Loop through {} cursor to map new coverages\n" \
                                   "\t\tOPEN {};\n" \
                                   "\t\tLOOP\n" \
                                    "\t\t\tFETCH {} INTO v_OLD_COVERAGE;\n" \
                                   "\t\t\tEXIT WHEN {}%NOTFOUND;\n".format(cov_cursor, cov_cursor, cov_cursor, cov_cursor)

                for child_coverage in coverage.child_coverages:
                    coverages_loops += "\n\t\t\t------------------------- INSERTING NEW CHILD COVERAGE ----------------------\n\n"
                    coverages_loops += QueryBuilder().insert_new_coverage(child_coverage, audit_id)

                    coverages_loops += QueryBuilder().insert_new_coverage_map(lob, "v_OLD_COVERAGE.COVERAGE", "v_COV_IDX", audit_id)

                    coverages_loops += "\n\t\t\t ------------------------ INSERTING COLs FOR CHILD COVERAGE ------------------------\n"
                    for cause in child_coverage.causes:
                        cause_of_loss_var = "v_" + cause.upper().replace(',', '').replace(' ', '_')
                        coverages_loops += QueryBuilder().insert_cclink("v_COV_IDX", cause_of_loss_var, audit_id)
                    coverages_loops += "\t\t\tv_COV_IDX := CIGADMIN.SEQ_COVERAGE.nextval;\n" \

                coverages_loops += "\t\tEND LOOP;\n" \
                                   "\t\tCLOSE {};\n".format(cov_cursor)
        return coverages_loops



    @staticmethod
    def insert_new_coverage(coverage, audit_id):
        return "\t\t\tINSERT INTO CIGADMIN.COVERAGE (COVERAGE, A_S_COVERAGE_LINE, CLASS, COVERAGE_DESC, " \
                "CREATE_ID, FIRST_MODIFIED, AUDIT_ID, LAST_MODIFIED, QUICK_CLAIMS_VALID)\n" \
                "\t\t\tVALUES(v_COV_IDX, v_OLD_COVERAGE.A_S_COVERAGE_LINE, v_OLD_COVERAGE.CLASS, " \
                "'{}', 'CIGADMIN', SYSDATE, '{}', SYSDATE, v_OLD_COVERAGE.QUICK_CLAIMS_VALID);\n" \
                "\t\t\tv_COVERAGES_INSERTED := v_COVERAGES_INSERTED + SQL%ROWCOUNT;\n\n" \
                .format(coverage.desc, audit_id)


    @staticmethod
    def insert_new_coverage_map(business_line, parent_coverage_idx_var, child_coverage_idx_var, audit_id):
        return "\t\t\tINSERT INTO CIGADMIN.CMS_COV_COV_MAP (BUSINESS_LINE, OLD_COVERAGE, NEW_COVERAGE, EFFECTIVE_DATE, CREATE_ID, " \
               "FIRST_MODIFIED, AUDIT_ID, LAST_MODIFIED, DEPRECATED_DATE)\n" \
               "\t\t\tVALUES ('{}', {}, {}, SYSDATE, 'CIGADMIN', SYSDATE, '{}', " \
               "SYSDATE, to_date('9999-01-01', 'yyyy-mm-dd'));\n" \
               "\t\t\tv_COV_MAP_INSERTED := v_COV_MAP_INSERTED + SQL%ROWCOUNT;\n" \
                .format(business_line, parent_coverage_idx_var, child_coverage_idx_var, audit_id)

    @staticmethod
    def insert_cclink(coverage_idx_var, cause_idx_var, audit_id):
        return  "\t\t\tINSERT INTO CIGADMIN.CC_LINK (CC_LINK, CAUSE_OF_LOSS, COVERAGE, CREATE_ID, FIRST_MODIFIED, AUDIT_ID, LAST_MODIFIED, EFFECTIVE_DATE, DEPRECATED_DATE)\n" \
                "\t\t\tVALUES (CIGADMIN.SEQ_CC_LINK.nextval, \n" \
                "\t\t\t{},\n" \
                "\t\t\t{},\n" \
                "\t\t\t'CIGADMIN',\n" \
                "\t\t\tSYSDATE,\n" \
                "\t\t\t'CMS-11860',\n" \
                "\t\t\tSYSDATE,\n" \
                "\t\t\tSYSDATE,\n" \
                "\t\t\t'1-JAN-9999'\n\t\t\t\t);\n" \
                "\t\t\tv_CCLINK_INSERTED := v_CCLINK_INSERTED + SQL%ROWCOUNT;\n\n" \
                 .format(cause_idx_var, coverage_idx_var)

