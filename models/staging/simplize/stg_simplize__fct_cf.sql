with
    source as (select * from {{ source("simplize", "fct_cf") }}),

    renamed as (
        select
            stock_code as fk_stock_id,
            quarter as quarter,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_kinh_doanh` as net_cash_flow_operating,
            `lợi_nhuận_trước_thuế` as pretax_profit,
            `điều_chỉnh_cho_các_khoản:` as adjustments,
            `khấu_hao_tài_sản_cố_định` as depreciation,
            `phân_bổ_lợi_thế_thương_mại` as goodwill_amortization,
            `các_khoản_dự_phòng` as provisions,
            `lãi/lỗ_chênh_lệch_tỷ_giá_hối_đoái` as forex_gain_loss,
            `lãi/lỗ_từ_thanh_lý_tài_sản_cố_định` as asset_disposal_gain_loss,
            `lãi/lỗ_từ_hoạt_động_đầu_tư` as investment_gain_loss,
            `chi_phí_lãi_vay` as interest_expense,
            `thu_lãi_và_cổ_tức` as interest_dividends_received,
            `các_khoản_điều_chỉnh_khác` as other_adjustments,
            `lợi_nhuận_từ_hoạt_động_kinh_doanh_trước_thay_đổi_vốn_lưu_động`
            as operating_profit_before_working_capital,
            `tăng/giảm_các_khoản_phải_thu` as change_in_receivables,
            `tăng/giảm_hàng_tồn_kho` as change_in_inventory,
            `tăng/giảm_các_khoản_phải_trả_không_kể_lãi_vay_phải_trả_thuế_thu_nhập_phải_nộp`
            as change_in_payables_excluding_interest_tax,
            `tăng/giảm_chi_phí_trả_trước` as change_in_prepaid_expenses,
            `tăng/giảm_chứng_khoán_kinh_doanh` as change_in_trading_securities,
            `thuế_thu_nhập_doanh_nghiệp_đã_nộp` as income_tax_paid,
            `lãi_vay_đã_trả` as interest_paid,
            `tiền_thu_khác_từ_hoạt_động_kinh_doanh` as other_operating_cash_inflows,
            `tiền_chi_khác_từ_hoạt_động_kinh_doanh` as other_operating_cash_outflows,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_đầu_tư` as net_cash_flow_investing,
            `tiền_chi_mua_sắm_xây_dựng_tài_sản_cố_định_và_các_tài_sản_dài_hạn_khác`
            as purchase_of_fixed_assets,
            `tiền_thu_từ_thanh_lý_nhượng_bán_tài_sản_cố_định_và_các_tài_sản_dài_hạn_khác`
            as proceeds_from_asset_disposal,
            `tiền_chi_cho_vay_mua_các_công_cụ_nợ_của_đơn_vị_khác`
            as loans_to_other_entities,
            `tiền_thu_hồi_cho_vay_bán_lại_các_công_cụ_nợ_của_đơn_vị_khác`
            as repayments_of_loans_to_other_entities,
            `tiền_chi_đầu_tư_góp_vốn_vào_đơn_vị_khác` as investments_in_other_entities,
            `tiền_thu_hồi_đầu_tư_góp_vốn_vào_đơn_vị_khác`
            as divestments_from_other_entities,
            `tiền_thu_lãi_cho_vay_cổ_tức_và_lợi_nhuận_được_chia`
            as interest_dividends_and_profit_distributions,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_tài_chính` as net_cash_flow_financing,
            `tiền_thu_phát_hành_cổ_phiếu_nhận_vốn_góp_của_chủ_sở_hữu`
            as proceeds_from_share_issuance,
            `tiền_chi_trả_vốn_góp_cho_các_chủ_sở_hữu_mua_lại_cổ_phiếu_của_doanh_nghiệp_đã_phát_hành`
            as payments_for_share_buybacks,
            `tiền_vay_ngắn_dài_hạn_được_nhận` as proceeds_from_borrowings,
            `tiền_chi_trả_nợ_gốc_vay` as repayments_of_borrowings,
            `tiền_chi_trả_nợ_thuê_tài_chính` as lease_liability_payments,
            `cổ_tức_lợi_nhuận_đã_trả_cho_chủ_sở_hữu` as dividends_paid,
            `lưu_chuyển_tiền_thuần_trong_kỳ` as net_cash_flow_for_period,
            `tiền_và_tương_đương_tiền_đầu_kỳ` as cash_and_cash_equivalents_beginning,
            `ảnh_hưởng_của_thay_đổi_tỷ_giá_hối_đoái_quy_đổi_ngoại_tệ`
            as effect_of_foreign_exchange_rate_changes,
            `tiền_và_tương_đương_tiền_cuối_kỳ` as cash_and_cash_equivalents_ending,
            `thu_nhập_lãi_và_các_khoản_thu_nhập_tương_tự_nhận_được`
            as interest_income_received,
            `chi_phí_lãi_và_các_chi_phí_tương_tự_đã_trả` as interest_expense_paid,
            `thu_nhập_từ_hoạt_động_dịch_vụ_nhận_được` as service_income_received,
            `chênh_lệch_số_tiền_thực_thu/thực_chi_từ_hoạt_động_kinh_doanh_ngoại_tệ_vàng_bạc_chứng_khoán`
            as net_cash_from_forex_gold_securities,
            `thu_nhập_khác` as other_income,
            `tiền_thu_các_khoản_nợ_đã_được_xử_lý_hóa_bù_đắp_bằng_nguồn_rủi_ro`
            as recoveries_of_written_off_debts,
            `tiền_chi_trả_cho_nhân_viên_và_hoạt_động_quản_lý_công_vụ`
            as payments_to_employees_and_management,
            `tiền_thuế_thu_nhập_thực_nộp_trong_kỳ` as income_tax_paid_during_period,
            `lưu_chuyển_tiền_thuần_từ_hoạt_động_kinh_doanh_trước_những_thay_đổi_về_tài_sản_và_vốn_lưu_động`
            as net_cash_flow_operating_before_changes,
            `những_thay_đổi_về_tài_sản_hoạt_động:` as changes_in_operating_assets,
            `tăng/giảm_các_khoản_tiền_vàng_gửi_và_cho_vay_các_tổ_chức_tín_dụng_khác`
            as change_in_gold_deposits_and_loans,
            `tăng/giảm_các_khoản_về_kinh_doanh_chứng_khoán`
            as change_in_securities_business,
            `tăng/giảm_các_công_cụ_tài_chính_phái_sinh_và_các_tài_sản_tài_chính_khác`
            as change_in_derivatives_and_other_financial_assets,
            `tăng/giảm_các_khoản_cho_vay_khách_hàng` as change_in_customer_loans,
            `giảm_nguồn_dự_phòng_để_bù_đắp_tổn_thất_các_khoản`
            as reduction_in_provision_sources,
            `tăng/giảm_khác_về_tài_sản_hoạt_động` as other_changes_in_operating_assets,
            `những_thay_đổi_về_công_nợ_hoạt_động:` as changes_in_operating_liabilities,
            `tăng/giảm_các_khoản_nợ_chính_phủ_và_ngân_hàng_nhà_nước`
            as change_in_government_and_central_bank_debts,
            `tăng/giảm_các_khoản_tiền_gửi_tiền_vay_các_tổ_chức_tín_dụng`
            as change_in_deposits_and_loans,
            `tăng/giảm_tiền_gửi_của_khách_hàng_bao_gồm_cả_kho_bạc_nhà_nước`
            as change_in_customer_deposits,
            `tăng/giảm_phát_hành_giấy_tờ_có_giá_ngoại_trừ_giấy_tờ_có_giá_phát_hành_được_tính_vào_hoạt_động_tài_chính`
            as change_in_issued_securities,
            `tăng/giảm_vốn_tài_trợ_ủy_thác_đầu_tư_cho_vay_mà_tổ_chức_tín_dụng_chịu_rủi_ro`
            as change_in_trust_investment_funds,
            `tăng/giảm_các_công_cụ_tài_chính_phái_sinh_và_các_khoản_nợ_tài_chính_khác`
            as change_in_derivatives_and_other_financial_liabilities,
            `tăng/giảm_lãi_phí_phải_trả` as change_in_interest_payables,
            `tăng/giảm_khác_về_công_nợ_hoạt_động`
            as other_changes_in_operating_liabilities,
            `chi_từ_các_quỹ_của_tổ_chức_tín_dụng` as disbursements_from_credit_funds,
            `tiền_chi_từ_thanh_lý_nhượng_bán_tài_sản_cố_định_và_các_tài_sản_dài_hạn_khác`
            as payments_for_asset_disposal,
            `mua_sắm_bất_động_sản_đầu_tư` as purchase_of_investment_properties,
            `tiền_thu_từ_bán_thanh_lý_bất_động_sản_đầu_tư`
            as proceeds_from_investment_property_disposal,
            `tiền_chi_ra_do_bán_thanh_lý_bất_động_sản_đầu_tư`
            as payments_for_investment_property_disposal,
            `tiền_thu_từ_phát_hành_giấy_tờ_có_giá_dài_hạn_có_đủ_điều_kiện_tính_vào_vốn_tự_có_và_các_khoản_vốn_vay_dài_hạn_khác`
            as proceeds_from_long_term_securities,
            `tiền_chi_thanh_toán_giấy_tờ_có_giá_dài_hạn_đủ_điều_kiện_tính_vào_vốn_tự_có_và_các_khoản_vốn_vay_dài_hạn_khác`
            as payments_for_long_term_securities,
            `tiền_chi_ra_mua_cổ_phiếu_quỹ` as purchase_of_treasury_shares,
            `tiền_thu_được_do_bán_cổ_phiếu_quỹ` as proceeds_from_treasury_shares,
            `lãi/lỗ_từ_hoạt_động_đầu_tư_đầu_tư_công_ty_con_liên_doanh_liên_kết`
            as investment_in_subsidiaries_gain_loss,
            `tiền_lãi_dự_thu` as accrued_interest_income,
            `tăng_các_chi_phí_phi_tiền_tệ:` as increase_in_non_monetary_expenses,
            `lỗ_đánh_giá_lại_giá_trị_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as fvtpl_assets_revaluation_loss,
            `lỗ_suy_giảm_giá_trị_các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`
            as htm_investment_impairment_loss,
            `lỗ_suy_giảm_giá_trị_các_khoản_cho_vay` as loan_impairment_loss,
            `lỗ_về_ghi_nhận_chênh_lệch_đánh_giá_theo_giá_trị_hợp_lý_tài_sản_tài_chính_sẵn_sàng_để_bán_afs_khi_phân_loại_lại`
            as afs_assets_revaluation_loss,
            `suy_giảm_giá_trị_của_các_tài_sản_cố_định_bất_động_sản_đầu_tư`
            as impairment_of_fixed_assets,
            `chi_phí_dự_phòng_suy_giảm_giá_trị_các_khoản_đầu_tư_tài_chính_dài_hạn`
            as long_term_investment_provision_expense,
            `lỗ_khác` as other_losses,
            `giảm_các_doanh_thu_phi_tiền_tệ:` as decrease_in_non_monetary_revenues,
            `lãi_đánh_giá_lại_giá_trị_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as fvtpl_assets_revaluation_gain,
            `lãi_về_ghi_nhận_chênh_lệch_đánh_giá_theo_giá_trị_hợp_lý_tài_sản_tài_chính_sẵn_sàng_để_bán_afs_khi_phân_loại_lại`
            as afs_assets_revaluation_gain,
            `lãi_khác` as other_gains,
            `thay_đổi_tài_sản_và_nợ_phải_trả_hoạt_động:`
            as changes_in_assets_and_liabilities,
            `tăng/giảm_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`
            as change_in_fvtpl_assets,
            `tăng/giảm_các_khoản_đầu_tư_nắm_giữa_đến_ngày_đáo_hạn_htm`
            as change_in_htm_investments,
            `tăng/giảm_các_khoản_cho_vay` as change_in_loans,
            `tăng/giảm_tài_sản_tài_chính_sẵn_sàng_để_bán_afs` as change_in_afs_assets,
            `tăng/giảm_phải_thu_bán_các_tài_sản_tài_chính`
            as change_in_receivables_from_asset_sales,
            `tăng/giảm_phải_thu_và_dự_thu_cổ_tức_tiền_lãi_các_tài_sản_tài_chính`
            as change_in_dividend_interest_receivables,
            `tăng/giảm_các_khoản_phải_thu_các_dịch_vụ_công_ty_chứng_khoán_cung_cấp`
            as change_in_receivables_from_services,
            `tăng/giảm_các_khoản_phải_thu_về_lỗi_giao_dịch_các_tài_sản_tài_chính`
            as change_in_receivables_from_transaction_errors,
            `tăng/giảm_các_khoản_phải_thu_khác` as change_in_other_receivables,
            `tăng/giảm_các_tài_sản_khác` as change_in_other_assets,
            `tăng/giảm_chi_phí_phải_trả_không_bao_gồm_chi_phí_lãi_vay`
            as change_in_accrued_expenses_excluding_interest,
            `tăng/giảm_phải_trả_cho_người_bán` as change_in_payables_to_suppliers,
            `tăng/giảm_các_khoản_trích_nộp_phúc_lợi_nhân_viên`
            as change_in_employee_benefits,
            `tăng/giảm_thuế_và_các_khoản_phải_nộp_nhà_nước_không_bao_gồm_thuế_thu_nhập_doanh_nghiệp_đã_nộp`
            as change_in_taxes_payable,
            `tăng/giảm_phải_trả_người_lao_động` as change_in_payables_to_employees,
            `tăng/giảm_phải_trả_về_lỗi_giao_dịch_các_tài_sản_tài_chính`
            as change_in_payables_from_transaction_errors,
            `tăng/giảm_phải_trả_phải_nộp_khác` as change_in_other_payables
        from source
    )

select *
from renamed
