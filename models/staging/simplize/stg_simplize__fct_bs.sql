with
    source as (select * from {{ source("simplize", "fct_bs") }}),

    renamed as (
        select 
            stock_code as stock_code,
            quarter as quarter,
            `tài_sản_ngắn_hạn` as current_assets,
            `tiền_và_các_khoản_tương_đương_tiền` as cash_and_cash_equivalents,
            `tiền` as cash,
            `các_khoản_tương_đương_tiền` as cash_equivalents,
            `đầu_tư_tài_chính_ngắn_hạn` as short_term_investments,
            `chứng_khoán_kinh_doanh` as trading_securities,
            `dự_phòng_giảm_giá_chứng_khoán_kinh_doanh` as trading_securities_provision,
            `đầu_tư_nắm_giữ_đến_ngày_đáo_hạn` as held_to_maturity_investments,
            `các_khoản_phải_thu_ngắn_hạn` as short_term_receivables,
            `phải_thu_ngắn_hạn_của_khách_hàng` as short_term_trade_receivables,
            `trả_trước_cho_người_bán_ngắn_hạn` as short_term_advance_to_suppliers,
            `phải_thu_nội_bộ_ngắn_hạn` as short_term_internal_receivables,
            `phải_thu_theo_tiến_độ_kế_hoạch_hợp_đồng_xây_dựng` as construction_contract_receivables,
            `phải_thu_về_cho_vay_ngắn_hạn` as short_term_loan_receivables,
            `phải_thu_ngắn_hạn_khác` as other_short_term_receivables,
            `dự_phòng_phải_thu_ngắn_hạn_khó_đòi` as short_term_bad_debt_provision,
            `tài_sản_thiếu_chờ_xử_lý` as pending_assets,
            `hàng_tồn_kho_ròng` as net_inventory,
            `hàng_tồn_kho` as inventory,
            `dự_phòng_giảm_giá_hàng_tồn_kho` as inventory_provision,
            `tài_sản_ngắn_hạn_khác` as other_current_assets,
            `chi_phí_trả_trước_ngắn_hạn` as short_term_prepaid_costs,
            `thuế_giá_trị_gia_tăng_được_khấu_trừ` as deductible_vat,
            `thuế_và_các_khoản_khác_phải_thu_của_nhà_nước` as tax_receivables,
            `giao_dịch_mua_bán_lại_trái_phiếu_chính_phủ` as government_bond_transactions,
            `tài_sản_dài_hạn` as non_current_assets,
            `các_khoản_phải_thu_dài_hạn` as long_term_receivables,
            `phải_thu_dài_hạn_của_khách_hàng` as long_term_trade_receivables,
            `trả_trước_cho_người_bán_dài_hạn` as long_term_advance_to_suppliers,
            `vốn_kinh_doanh_ở_các_đơn_vị_trực_thuộc` as business_capital_in_subsidiaries,
            `phải_thu_nội_bộ_dài_hạn` as long_term_internal_receivables,
            `phải_thu_về_cho_vay_dài_hạn` as long_term_loan_receivables,
            `phải_thu_dài_hạn_khác` as other_long_term_receivables,
            `dự_phòng_phải_thu_dài_hạn_khó_đòi` as long_term_bad_debt_provision,
            `tài_sản_cố_định` as fixed_assets,
            `tài_sản_cố_định_hữu_hình` as tangible_fixed_assets,
            `nguyên_giá_tscđ_hữu_hình` as tangible_fixed_assets_cost,
            `khấu_hao_lũy_kế_tscđ_hữu_hình` as tangible_fixed_assets_depreciation,
            `tài_sản_cố_định_thuê_tài_chính` as finance_leased_assets,
            `nguyên_giá_tscđ_thuê_tài_chính` as finance_leased_assets_cost,
            `khấu_hao_lũy_kế_tscđ_thuê_tài_chính` as finance_leased_assets_depreciation,
            `tài_sản_cố_định_vô_hình` as intangible_fixed_assets,
            `nguyên_giá_tscđ_vô_hình` as intangible_fixed_assets_cost,
            `khấu_hao_lũy_kế_tscđ_vô_hình` as intangible_fixed_assets_depreciation,
            `bất_động_sản_đầu_tư` as investment_properties,
            `nguyên_giá_bđs_đầu_tư` as investment_properties_cost,
            `khấu_hao_lũy_kế_bđs_đầu_tư` as investment_properties_depreciation,
            `tài_sản_dở_dang_dài_hạn` as long_term_construction_in_progress,
            `chi_phí_sản_xuất_kinh_doanh_dở_dang_dài_hạn` as long_term_business_construction_costs,
            `chi_phí_xây_dựng_cơ_bản_dở_dang` as basic_construction_in_progress,
            `đầu_tư_tài_chính_dài_hạn` as long_term_financial_investments,
            `đầu_tư_vào_công_ty_con` as investments_in_subsidiaries,
            `đầu_tư_vào_công_ty_liên_kết_liên_doanh` as investments_in_associates,
            `đầu_tư_dài_hạn_khác` as other_long_term_investments,
            `dự_phòng_đầu_tư_tài_chính_dài_hạn` as long_term_financial_investment_provision,
            `tài_sản_dài_hạn_khác` as other_non_current_assets,
            `chi_phí_trả_trước_dài_hạn` as long_term_prepaid_costs,
            `tài_sản_thuế_thu_nhập_hoãn_lại` as deferred_tax_assets,
            `thiết_bị_vật_tư_phụ_tùng_thay_thế_dài_hạn` as long_term_spare_parts,
            `lợi_thế_thương_mại` as goodwill,
            `tổng_tài_sản` as total_assets,
            `nợ_phải_trả` as liabilities,
            `nợ_ngắn_hạn` as current_liabilities,
            `vay_và_nợ_thuê_tài_chính_ngắn_hạn` as short_term_borrowings,
            `phải_trả_người_bán_ngắn_hạn` as short_term_trade_payables,
            `người_mua_trả_tiền_trước_ngắn_hạn` as short_term_advance_from_customers,
            `thuế_và_các_khoản_phải_nộp_nhà_nước` as taxes_payable,
            `phải_trả_người_lao_động` as payables_to_employees,
            `chi_phí_phải_trả_ngắn_hạn` as short_term_accrued_expenses,
            `phải_trả_nội_bộ_ngắn_hạn` as short_term_internal_payables,
            `phải_trả_theo_tiến_độ_kế_hoạch_hợp_đồng_xây_dựng` as construction_contract_payables,
            `doanh_thu_chưa_thực_hiện_ngắn_hạn` as short_term_unearned_revenue,
            `phải_trả_khác` as other_payables,
            `dự_phòng_các_khoản_phải_trả_ngắn_hạn` as short_term_provisions,
            `quỹ_khen_thưởng_phúc_lợi` as bonus_and_welfare_fund,
            `quỹ_bình_ổn_giá` as price_stabilization_fund,
            `nợ_dài_hạn` as long_term_liabilities,
            `vay_và_nợ_thuê_tài_chính_dài_hạn` as long_term_borrowings,
            `phải_trả_nhà_cung_cấp_dài_hạn` as long_term_trade_payables,
            `người_mua_trả_tiền_trước_dài_hạn` as long_term_advance_from_customers,
            `chi_phí_phải_trả_dài_hạn` as long_term_accrued_expenses,
            `phải_trả_nội_bộ_về_vốn_kinh_doanh` as internal_payables_for_business_capital,
            `phải_trả_nội_bộ_dài_hạn` as long_term_internal_payables,
            `doanh_thu_chưa_thực_hiện_dài_hạn` as long_term_unearned_revenue,
            `phải_trả_dài_hạn_khác` as other_long_term_payables,
            `trái_phiếu_chuyển_đổi` as convertible_bonds,
            `cổ_phiếu_ưu_đãi_nợ` as debt_preferred_shares,
            `thuế_thu_nhập_hoãn_lại_phải_trả` as deferred_tax_liabilities,
            `dự_phòng_phải_trả_dài_hạn` as long_term_provisions,
            `quỹ_phát_triển_khoa_học_và_công_nghệ` as science_and_technology_development_fund,
            `dự_phòng_trợ_cấp_mất_việc` as severance_provision,
            `vốn_chủ_sở_hữu` as equity,
            `vốn_góp_của_chủ_sở_hữu` as contributed_capital,
            `cổ_phiếu_phổ_thông_có_quyền_biểu_quyết` as ordinary_shares,
            `cổ_phiếu_ưu_đãi` as preferred_shares,
            `thặng_dư_vốn_cổ_phần` as share_premium,
            `quyền_chọn_chuyển_đổi_trái_phiếu` as bond_conversion_rights,
            `vốn_khác_của_chủ_sở_hữu` as other_equity,
            `cổ_phiếu_quỹ` as treasury_shares,
            `chênh_lệch_đánh_giá_lại_tài_sản` as asset_revaluation_reserve,
            `chênh_lệch_tỷ_giá_hối_đoái` as foreign_exchange_difference,
            `quỹ_đầu_tư_phát_triển` as development_investment_fund,
            `quỹ_hỗ_trợ_sắp_xếp_doanh_nghiệp` as business_arrangement_support_fund,
            `quỹ_khác_thuộc_vốn_chủ_sở_hữu` as other_equity_funds,
            `lợi_nhuận_sau_thuế_chưa_phân_phối` as undistributed_profit_after_tax,
            `lnst_chưa_phân_phối_lũy_kế_đến_cuối_kỳ_trước` as cumulative_undistributed_profit_previous_period,
            `lnst_chưa_phân_phối_kỳ_này` as undistributed_profit_current_period,
            `lợi_ích_cổ_đông_không_kiểm_soát` as non_controlling_interests,
            `nguồn_kinh_phí_và_quỹ_khác` as other_funds,
            `tổng_nguồn_vốn` as total_equity_and_liabilities,
            `tiền_mặt_vàng_bạc_đá_quý` as cash_gold_silver_gems,
            `tiền_gửi_tại_ngân_hàng_nhà_nước` as deposits_at_central_bank,
            `tiền_vàng_gửi_tại_các_tổ_chức_tín_dụng_khác_và_cho_vay_các_tổ_chức_tín_dụng_khác` as deposits_and_loans_to_other_credit_institutions,
            `các_công_cụ_tài_chính_phái_sinh_và_các_tài_sản_tài_chính_khác` as derivatives_and_other_financial_assets,
            `cho_vay_khách_hàng` as loans_to_customers,
            `cho_vay_và_cho_thuê_tài_chính_khách_hàng` as finance_leases_to_customers,
            `dự_phòng_rủi_ro_cho_vay_và_cho_thuê_tài_chính_khách_hàng` as loan_and_finance_lease_provision,
            `hoạt_động_mua_nợ` as debt_purchase_activity,
            `mua_nợ` as debt_purchase,
            `dự_phòng_rủi_ro_hoạt_động_mua_nợ` as debt_purchase_provision,
            `chứng_khoán_đầu_tư` as investment_securities,
            `chứng_khoán_đầu_tư_sẵn_sàng_để_bán` as available_for_sale_securities,
            `chứng_khoán_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn` as held_to_maturity_securities,
            `dự_phòng_giảm_giá_chứng_khoán_đầu_tư` as investment_securities_provision,
            `góp_vốn_đầu_tư_dài_hạn` as long_term_investment_contributions,
            `dự_phòng_giảm_giá_đầu_tư_dài_hạn` as long_term_investment_devaluation_provision,
            `đầu_tư_vào_công_ty_liên_doanh_liên_kết` as investments_in_joint_ventures,
            `tài_sản_“có”_khác` as other_assets,
            `tổng_nợ_phải_trả` as total_liabilities,
            `các_khoản_nợ_chính_phủ_và_ngân_hàng_nhà_nước` as government_and_central_bank_debts,
            `tiền_gửi_và_vay_các_tổ_chức_tín_dụng_khác` as deposits_and_loans_from_other_credit_institutions,
            `tiền_gửi_của_khách_hàng` as customer_deposits,
            `các_công_cụ_tài_chính_phái_sinh_và_các_khoản_nợ_tài_chính_khác` as derivatives_and_other_financial_liabilities,
            `vốn_tài_trợ_ủy_thác_đầu_tư_cho_vay_mà_tổ_chức_tín_dụng_chịu_rủi_ro` as trust_investment_funds,
            `phát_hành_giấy_tờ_có_giá` as issued_securities,
            `các_khoản_nợ_khác` as other_liabilities,
            `vốn_của_tổ_chức_tín_dụng` as credit_institution_capital,
            `vốn_điều_lệ` as charter_capital,
            `vốn_đầu_tư_xây_dựng_cơ_bản` as basic_construction_investment_capital,
            `vốn_khác` as other_capital,
            `quỹ_của_tổ_chức_tín_dụng` as credit_institution_fund,
            `giá_trị_thuần_đầu_tư_tài_sản_tài_chính_ngắn_hạn` as net_value_of_short_term_financial_assets,
            `các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl` as fvtpl_financial_assets,
            `các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm` as htm_investments,
            `các_khoản_cho_vay` as loans,
            `các_khoản_tài_chính_sẵn_sàng_để_bán_afs` as afs_financial_assets,
            `tổng_các_khoản_phải_thu` as total_receivables,
            `các_khoản_phải_thu` as receivables,
            `phải_thu_các_dịch_vụ_công_ty_chứng_khoán_cung_cấp` as receivables_from_securities_services,
            `phải_thu_về_lỗi_giao_dịch_chứng_khoán` as receivables_from_securities_transaction_errors,
            `phải_thu_về_hoạt_động_giao_dịch_chứng_khoán` as receivables_from_securities_trading,
            `phải_thu_về_xây_dựng_cơ_bản` as receivables_from_construction,
            `dự_phòng_suy_giảm_giá_trị_các_khoản_phải_thu` as receivables_impairment_provision,
            `dự_phòng_nợ_khó_đòi` as bad_debt_provision,
            `vật_tư_văn_phòng_công_cụ_dụng_cụ` as office_supplies,
            `các_khoản_cầm_cố_ký_cược_ký_quỹ` as collateral_and_deposits,
            `phải_thu_thuế_khác` as other_tax_receivables,
            `dự_phòng_suy_giảm_giá_trị_tài_sản_ngắn_hạn_khác` as other_current_assets_provision,
            `dự_phòng_giảm_giá_tài_sản_tài_chính_dài_hạn` as long_term_financial_assets_provision,
            `đầu_tư_dài_hạn` as long_term_investments,
            `đầu_tư_chứng_khoán_dài_hạn` as long_term_securities_investments,
            `cầm_cố_thế_chấp_ký_quỹ_và_ký_cược_dài_hạn` as long_term_pledges_morgage_and_deposits,
            `tiền_chi_nộp_quỹ_hỗ_trợ_thanh_toán` as payments_to_settlement_support_fund,
            `dự_phòng_suy_giảm_giá_trị_tài_sản_dài_hạn` as long_term_assets_provision,
            `vay_tài_sản_tài_chính_ngắn_hạn` as short_term_financial_asset_loans,
            `trái_phiếu_chuyển_đổi_ngắn_hạn` as short_term_convertible_bonds,
            `trái_phiếu_phát_hành_ngắn_hạn` as short_term_issued_bonds,
            `vay_quỹ_hỗ_trợ_thanh_toán` as loans_from_settlement_support_fund,
            `phải_trả_hoạt_động_giao_dịch_chứng_khoán` as payables_from_securities_trading,
            `phải_trả_về_lỗi_giao_dịch_các_tài_sản_tài_chính` as payables_from_financial_transaction_errors,
            `các_khoản_phải_trả_về_thuế` as tax_payables,
            `các_khoản_trích_nộp_phúc_lợi_nhân_viên` as employee_benefits_payables,
            `nhận_ký_quỹ_ký_cược_ngắn_hạn` as short_term_collateral_received,
            `phải_trả_về_xây_dựng_cơ_bản` as payables_from_construction,
            `phải_trả_cổ_tức_gốc_và_lãi_trái_phiếu` as dividend_and_bond_interest_payables,
            `phải_trả_tổ_chức_phát_hành_chứng_khoán` as payables_to_securities_issuers,
            `vay_tài_sản_tài_chính_dài_hạn` as long_term_financial_asset_loans,
            `trái_phiếu_chuyển_đổi_dài_hạn` as long_term_convertible_bonds,
            `trái_phiếu_phát_hành_dài_hạn` as long_term_issued_bonds,
            `ký_quỹ_ký_cược_dài_hạn` as long_term_collateral_and_deposits,
            `quỹ_bảo_vệ_nhà_đầu_tư` as investor_protection_fund,
            `quỹ_dự_trữ_điều_lệ` as statutory_reserve_fund,
            `quỹ_dự_phòng_tài_chính` as financial_reserve_fund,
            `phải_thu_về_hợp_đồng_bảo_hiểm` as insurance_contract_receivables,
            `phải_thu_khác_của_khách_hàng` as other_customer_receivables,
            `tạm_ứng` as advances,
            `phải_thu_từ_hoạt_động_đầu_tư_tài_chính` as receivables_from_financial_investments,
            `chi_phí_hoa_hồng_chưa_phân_bổ` as unallocated_commission_expenses,
            `chi_phí_trả_trước_ngắn_hạn_khác` as other_short_term_prepaid_costs,
            `tài_sản_tái_bảo_hiểm` as reinsurance_assets,
            `dự_phòng_phí_nhượng_tái_bảo_hiểm` as reinsurance_fee_provision,
            `dự_phòng_bồi_thường_nhượng_tái_bảo_hiểm` as reinsurance_compensation_provision,
            `ký_quỹ_bảo_hiểm` as insurance_collateral,
            `tài_sản_ký_quỹ_dài_hạn` as long_term_collateral_assets,
            `vay_và_nợ_ngắn_hạn` as short_term_loans_and_debts,
            `phải_trả_thương_mại` as trade_payables,
            `phải_trả_về_hợp_đồng_bảo_hiểm` as insurance_contract_payables,
            `phải_trả_khác_cho_người_bán` as other_payables_to_sellers,
            `doanh_thu_hoa_hồng_chưa_được_hưởng` as unearned_commission_revenue,
            `dự_phòng_nghiệp_vụ_bảo_hiểm` as insurance_provision,
            `dự_phòng_phí_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm` as original_and_reinsurance_fee_provision,
            `dự_phòng_toán_học` as mathematical_provision,
            `dự_phòng_bồi_thường_bảo_hiểm_gốc_nhận_tái_bảo_hiểm` as original_and_reinsurance_compensation_provision,
            `dự_phòng_dao_động_lớn` as large_fluctuation_provision,
            `dự_phòng_chia_lãi` as profit_sharing_provision,
            `dự_phòng_đảm_bảo_cân_đối` as balance_guarantee_provision,
            `quỹ_dự_trữ_bắt_buộc` as mandatory_reserve_fund
        from source
    )

select *
from renamed