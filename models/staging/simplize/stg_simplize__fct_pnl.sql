with
    source as (select * from {{ source("simplize", "fct_pnl") }}),

    renamed as (
        select
            stock_code as fk_stock_id,
            quarter as fk_quarter_id,
            coalesce(
                `doanh_thu_bán_hàng_và_cung_cấp_dịch_vụ`, 0
            ) as doanh_thu_ban_hang_va_cung_cap_dich_vu,
            coalesce(`các_khoản_giảm_trừ_doanh_thu`, 0) as cac_khoan_giam_tru_doanh_thu,
            coalesce(
                `doanh_thu_thuần_về_bán_hàng_và_cung_cấp_dịch_vụ`, 0
            ) as doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu,
            coalesce(`giá_vốn_hàng_bán`, 0) as gia_von_hang_ban,
            coalesce(
                `lợi_nhuận_gộp_về_bán_hàng_và_cung_cấp_dịch_vụ`, 0
            ) as loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu,
            coalesce(
                `doanh_thu_hoạt_động_tài_chính`, 0
            ) as doanh_thu_hoat_dong_tai_chinh,
            coalesce(`chi_phí_tài_chính`, 0) as chi_phi_tai_chinh,
            coalesce(`chi_phí_lãi_vay`, 0) as chi_phi_lai_vay,
            coalesce(
                `lợi_nhuận_từ_công_ty_liên_doanh_liên_kết`, 0
            ) as loi_nhuan_tu_cong_ty_lien_doanh_lien_ket,
            coalesce(`chi_phí_bán_hàng`, 0) as chi_phi_ban_hang,
            coalesce(`chi_phí_quản_lý_doanh_nghiệp`, 0) as chi_phi_quan_ly_doanh_nghiep,
            coalesce(
                `lợi_nhuận_thuần_từ_hoạt_động_kinh_doanh`, 0
            ) as loi_nhuan_thuan_tu_hoat_dong_kinh_doanh,
            coalesce(`lợi_nhuận_khác`, 0) as loi_nhuan_khac,
            coalesce(`thu_nhập_khác`, 0) as thu_nhap_khac,
            coalesce(`chi_phí_khác`, 0) as chi_phi_khac,
            coalesce(`lợi_nhuận_kế_toán_trước_thuế`, 0) as loi_nhuan_ke_toan_truoc_thue,
            coalesce(`thuế_thu_nhập_doanh_nghiệp`, 0) as thue_thu_nhap_doanh_nghiep,
            coalesce(
                `chi_phí_thuế_thu_nhập_hiện_hành`, 0
            ) as chi_phi_thue_thu_nhap_hien_hanh,
            coalesce(
                `chi_phí_thuế_thu_nhập_hoãn_lại`, 0
            ) as chi_phi_thue_thu_nhap_hoan_lai,
            coalesce(`lợi_nhuận_kế_toán_sau_thuế`, 0) as loi_nhuan_ke_toan_sau_thue,
            coalesce(`lợi_nhuận_cổ_đông_công_ty_mẹ`, 0) as loi_nhuan_co_dong_cong_ty_me,
            coalesce(`lợi_nhuận_cổ_đông_thiểu_số`, 0) as loi_nhuan_co_dong_thieu_so,
            coalesce(`thu_nhập_lãi_thuần`, 0) as thu_nhap_lai_thuan,
            coalesce(
                `thu_nhập_lãi_và_các_khoản_thu_nhập_tương_tự`, 0
            ) as thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu,
            coalesce(
                `chi_phí_lãi_và_các_chi_phí_tương_tự`, 0
            ) as chi_phi_lai_va_cac_chi_phi_tuong_tu,
            coalesce(
                `lãi/lỗ_thuần_từ_hoạt_động_dịch_vụ`, 0
            ) as lai_lo_thuan_tu_hoat_dong_dich_vu,
            coalesce(
                `thu_nhập_từ_hoạt_động_dịch_vụ`, 0
            ) as thu_nhap_tu_hoat_dong_dich_vu,
            coalesce(`chi_phí_hoạt_động_dịch_vụ`, 0) as chi_phi_hoat_dong_dich_vu,
            coalesce(
                `lãi/lỗ_thuần_từ_hoạt_động_kinh_doanh_ngoại_hối_và_vàng`, 0
            ) as lai_lo_thuan_tu_hoat_dong_kinh_doanh_ngoai_hoi_va_vang,
            coalesce(
                `lãi/lỗ_thuần_từ_mua_bán_chứng_khoán_kinh_doanh`, 0
            ) as lai_lo_thuan_tu_mua_ban_chung_khoan_kinh_doanh,
            coalesce(
                `lãi/lỗ_thuần_từ_mua_bán_chứng_khoán_đầu_tư`, 0
            ) as lai_lo_thuan_tu_mua_ban_chung_khoan_dau_tu,
            coalesce(
                `lãi/lỗ_thuần_từ_hoạt_động_khác`, 0
            ) as lai_lo_thuan_tu_hoat_dong_khac,
            coalesce(`thu_nhập_từ_hoạt_động_khác`, 0) as thu_nhap_tu_hoat_dong_khac,
            coalesce(`chi_phí_hoạt_động_khác`, 0) as chi_phi_hoat_dong_khac,
            coalesce(
                `thu_nhập_từ_góp_vốn_mua_cổ_phần`, 0
            ) as thu_nhap_tu_gop_von_mua_co_phan,
            coalesce(`tổng_thu_nhập_hoạt_động`, 0) as tong_thu_nhap_hoat_dong,
            coalesce(`chi_phí_hoạt_động`, 0) as chi_phi_hoat_dong,
            coalesce(
                `lợi_nhuận_thuần_từ_hoạt_động_kinh_doanh_trước_chi_phí_dự_phòng_rủi_ro_tín_dụng`,
                0
            )
            as loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truoc_chi_phi_du_phong_rui_ro_tin_dung,
            coalesce(
                `chi_phí_dự_phòng_rủi_ro_tín_dụng`, 0
            ) as chi_phi_du_phong_rui_ro_tin_dung,
            coalesce(`doanh_thu_hoạt_động`, 0) as doanh_thu_hoat_dong,
            coalesce(
                `lãi_từ_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi/lỗ_fvtpl`, 0
            ) as lai_tu_cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            coalesce(
                `lãi_bán_các_tài_sản_tài_chính`, 0
            ) as lai_ban_cac_tai_san_tai_chinh,
            coalesce(
                `chênh_lệch_tăng_đánh_giá_lại_các_tài_sản_tài_chính_thông_qua_lãi/lỗ`, 0
            ) as chenh_lech_tang_danh_gia_lai_cac_tai_san_tai_chinh_thong_qua_lai_lo,
            coalesce(
                `cổ_tức_tiền_lãi_phát_sinh_từ_tài_sản_tài_chính_fvtpl`, 0
            ) as co_tuc_tien_lai_phat_sinh_tu_tai_san_tai_chinh_fvtpl,
            coalesce(
                `lãi_từ_các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`, 0
            ) as lai_tu_cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            coalesce(
                `lãi_từ_các_khoản_cho_vay_và_phải_thu`, 0
            ) as lai_tu_cac_khoan_cho_vay_va_phai_thu,
            coalesce(
                `lãi_từ_các_tài_sản_tài_chính_sẵn_sàng_để_bán_afs`, 0
            ) as lai_tu_cac_tai_san_tai_chinh_san_sang_de_ban_afs,
            coalesce(
                `lãi_từ_các_công_cụ_phát_sinh_phòng_ngừa_rủi_ro`, 0
            ) as lai_tu_cac_cong_cu_phat_sinh_phong_ngua_rui_ro,
            coalesce(
                `doanh_thu_từ_hoạt_động_môi_giới_chứng_khoán`, 0
            ) as doanh_thu_tu_hoat_dong_moi_gioi_chung_khoan,
            coalesce(
                `doanh_thu_bảo_lãnh_phát_hành_chứng_khoán`, 0
            ) as doanh_thu_bao_lanh_phat_hanh_chung_khoan,
            coalesce(
                `doanh_thu_đại_lý_phát_hành_chứng_khoán`, 0
            ) as doanh_thu_dai_ly_phat_hanh_chung_khoan,
            coalesce(
                `doanh_thu_hoạt_động_tư_vấn_đầu_tư_chứng_khoán`, 0
            ) as doanh_thu_hoat_dong_tu_van_dau_tu_chung_khoan,
            coalesce(
                `doanh_thu_hoạt_động_ủy_thác_đấu_giá`, 0
            ) as doanh_thu_hoat_dong_uy_thac_dau_gia,
            coalesce(`doanh_thu_lưu_ký_chứng_khoán`, 0) as doanh_thu_luu_ky_chung_khoan,
            coalesce(
                `doanh_thu_hoạt_động_đầu_tư_chứng_khoán_góp_vốn`, 0
            ) as doanh_thu_hoat_dong_dau_tu_chung_khoan_gop_von,
            coalesce(`thu_cho_thuê_sử_dụng_tài_sản`, 0) as thu_cho_thue_su_dung_tai_san,
            coalesce(
                `doanh_thu_hoạt_động_tư_vấn_tài_chính`, 0
            ) as doanh_thu_hoat_dong_tu_van_tai_chinh,
            coalesce(`doanh_thu_khác`, 0) as doanh_thu_khac,
            coalesce(`doanh_thu_thuần`, 0) as doanh_thu_thuan,
            coalesce(`chi_phí_hoạt_động_kinh_doanh`, 0) as chi_phi_hoat_dong_kinh_doanh,
            coalesce(
                `lỗ_các_tài_sản_tài_chính_ghi_nhận_thông_qua_lãi_lỗ_fvtpl`, 0
            ) as lo_cac_tai_san_tai_chinh_ghi_nhan_thong_qua_lai_lo_fvtpl,
            coalesce(`lỗ_bán_các_tài_sản_tài_chính`, 0) as lo_ban_cac_tai_san_tai_chinh,
            coalesce(
                `chênh_lệch_giảm_đánh_giá_lại_các_tài_sản_tài_chính_fvtpl`, 0
            ) as chenh_lech_giam_danh_gia_lai_cac_tai_san_tai_chinh_fvtpl,
            coalesce(
                `chi_phí_giao_dịch_mua_các_tài_sản_tài_chính_fvtpl`, 0
            ) as chi_phi_giao_dich_mua_cac_tai_san_tai_chinh_fvtpl,
            coalesce(
                `lỗ_các_khoản_đầu_tư_nắm_giữ_đến_ngày_đáo_hạn_htm`, 0
            ) as lo_cac_khoan_dau_tu_nam_giu_den_ngay_dao_han_htm,
            coalesce(
                `chi_phí_lãi_vay_lỗ_từ_các_khoản_cho_vay_và_phải_thu`, 0
            ) as chi_phi_lai_vay_lo_tu_cac_khoan_cho_vay_va_phai_thu,
            coalesce(
                `lỗ_và_ghi_nhận_chênh_lệch_đánh_giá_theo_giá_trị_hợp_lý_tài_sản_tài_chính_sẵn_sàng_để_bán_afs`,
                0
            )
            as lo_va_ghi_nhan_chenh_lech_danh_gia_theo_gia_tri_hop_ly_tai_san_tai_chinh_san_sang_de_ban_afs,
            coalesce(
                `chi_phí_dự_phòng_tài_sản_tài_chính_xử_lý_tổn_thất_các_khoản_phải_thu_khó_đòi_và_lỗ_suy_giảm_tài_sản_tài_chính_và_cổ_phiếu_đi_vay`,
                0
            )
            as chi_phi_du_phong_tai_san_tai_chinh_xu_ly_ton_that_cac_khoan_phai_thu_kho_doi_va_lo_suy_giam_tai_san_tai_chinh_va_co_phieu_di_vay,
            coalesce(
                `lỗ_từ_các_tài_sản_tài_chính_phái_sinh_phòng_ngừa_rủi_ro`, 0
            ) as lo_tu_cac_tai_san_tai_chinh_phai_sinh_phong_ngua_rui_ro,
            coalesce(`chi_phí_hoạt_động_tự_doanh`, 0) as chi_phi_hoat_dong_tu_doanh,
            coalesce(`chi_phí_môi_giới_chứng_khoán`, 0) as chi_phi_moi_gioi_chung_khoan,
            coalesce(
                `chi_phí_hoạt_động_bảo_lãnh_đại_lý_phát_hành_chứng_khoán`, 0
            ) as chi_phi_hoat_dong_bao_lanh_dai_ly_phat_hanh_chung_khoan,
            coalesce(
                `chi_phí_tư_vấn_đầu_tư_chứng_khoán`, 0
            ) as chi_phi_tu_van_dau_tu_chung_khoan,
            coalesce(
                `chi_phí_hoạt_động_đấu_giá_ủy_thác`, 0
            ) as chi_phi_hoat_dong_dau_gia_uy_thac,
            coalesce(`chi_phí_lưu_ký_chứng_khoán`, 0) as chi_phi_luu_ky_chung_khoan,
            coalesce(
                `chi_phí_hoạt_động_tư_vấn_tài_chính`, 0
            ) as chi_phi_hoat_dong_tu_van_tai_chinh,
            coalesce(`chi_phí_các_dịch_vụ_khác`, 0) as chi_phi_cac_dich_vu_khac,
            coalesce(`lợi_nhuận_gộp`, 0) as loi_nhuan_gop,
            coalesce(
                `chênh_lệch_lãi_tỷ_giá_hối_đoái_đã_và_chưa_thực_hiện`, 0
            ) as chenh_lech_lai_ty_gia_hoi_doai_da_va_chua_thuc_hien,
            coalesce(
                `doanh_thu_dự_thu_cổ_tức_lãi_tiền_gửi_không_cố_định_phát_sinh_trong_kỳ`,
                0
            ) as doanh_thu_du_thu_co_tuc_lai_tien_gui_khong_co_dinh_phat_sinh_trong_ky,
            coalesce(
                `lãi_bán_thanh_lý_các_khoản_đầu_tư_vào_công_ty_con_liên_kết_liên_doanh`,
                0
            ) as lai_ban_thanh_ly_cac_khoan_dau_tu_vao_cong_ty_con_lien_ket_lien_doanh,
            coalesce(`doanh_thu_khác_về_đầu_tư`, 0) as doanh_thu_khac_ve_dau_tu,
            coalesce(
                `chênh_lệch_lỗ_tỷ_giá_hối_đoái_đã_và_chưa_thực_hiện`, 0
            ) as chenh_lech_lo_ty_gia_hoi_doai_da_va_chua_thuc_hien,
            coalesce(
                `lỗ_bán_thanh_lý_các_khoản_đầu_tư_vào_công_ty_con_liên_kết_liên_doanh`,
                0
            ) as lo_ban_thanh_ly_cac_khoan_dau_tu_vao_cong_ty_con_lien_ket_lien_doanh,
            coalesce(
                `chi_phí_dự_phòng_suy_giảm_giá_trị_các_khoản_đầu_tư_tài_chính_dài_hạn`,
                0
            ) as chi_phi_du_phong_suy_giam_gia_tri_cac_khoan_dau_tu_tai_chinh_dai_han,
            coalesce(`chi_phí_tài_chính_khác`, 0) as chi_phi_tai_chinh_khac,
            coalesce(
                `chi_phí_quản_lý_công_ty_chứng_khoán`, 0
            ) as chi_phi_quan_ly_cong_ty_chung_khoan,
            coalesce(`kết_quả_hoạt_động`, 0) as ket_qua_hoat_dong,
            coalesce(`doanh_thu_phí_bảo_hiểm`, 0) as doanh_thu_phi_bao_hiem,
            coalesce(`phí_bảo_hiểm_gốc`, 0) as phi_bao_hiem_goc,
            coalesce(`phí_nhận_tái_bảo_hiểm`, 0) as phi_nhan_tai_bao_hiem,
            coalesce(
                `tăng_dự_phòng_phí_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm`, 0
            ) as tang_du_phong_phi_bao_hiem_goc_va_nhan_tai_bao_hiem,
            coalesce(`phí_nhượng_tái_bảo_hiểm`, 0) as phi_nhuong_tai_bao_hiem,
            coalesce(`tổng_phí_nhượng_tái_bảo_hiểm`, 0) as tong_phi_nhuong_tai_bao_hiem,
            coalesce(
                `tăng_dự_phòng_phí_nhượng_tái_bảo_hiểm`, 0
            ) as tang_du_phong_phi_nhuong_tai_bao_hiem,
            coalesce(`doanh_thu_phí_bảo_hiểm_thuần`, 0) as doanh_thu_phi_bao_hiem_thuan,
            coalesce(
                `hoa_hồng_nhượng_tái_bảo_hiểm_và_doanh_thu_khác_hoạt_động_kinh_doanh_bảo_hiểm`,
                0
            )
            as hoa_hong_nhuong_tai_bao_hiem_va_doanh_thu_khac_hoat_dong_kinh_doanh_bao_hiem,
            coalesce(`hoa_hồng_nhượng_tái_bảo_hiểm`, 0) as hoa_hong_nhuong_tai_bao_hiem,
            coalesce(
                `doanh_thu_khác_hoạt_động_kinh_doanh_bảo_hiểm`, 0
            ) as doanh_thu_khac_hoat_dong_kinh_doanh_bao_hiem,
            coalesce(
                `doanh_thu_thuần_hoạt_động_kinh_doanh_bảo_hiểm`, 0
            ) as doanh_thu_thuan_hoat_dong_kinh_doanh_bao_hiem,
            coalesce(`chi_bồi_thường`, 0) as chi_boi_thuong,
            coalesce(`tổng_chi_bồi_thường`, 0) as tong_chi_boi_thuong,
            coalesce(`các_khoản_giảm_trừ`, 0) as cac_khoan_giam_tru,
            coalesce(
                `thu_bồi_thường_nhượng_tái_bảo_hiểm`, 0
            ) as thu_boi_thuong_nhuong_tai_bao_hiem,
            coalesce(`tăng/giảm_dự_phòng_toán_học`, 0) as tang_giam_du_phong_toan_hoc,
            coalesce(
                `tăng_dự_phòng_bồi_thường_bảo_hiểm_gốc_và_nhận_tái_bảo_hiểm`, 0
            ) as tang_du_phong_boi_thuong_bao_hiem_goc_va_nhan_tai_bao_hiem,
            coalesce(
                `tăng_dự_phòng_bồi_thường_nhượng_tái_bảo_hiểm`, 0
            ) as tang_du_phong_boi_thuong_nhuong_tai_bao_hiem,
            coalesce(`tổng_chi_bồi_thường_bảo_hiểm`, 0) as tong_chi_boi_thuong_bao_hiem,
            coalesce(`tăng_dự_phòng_dao_động_lớn`, 0) as tang_du_phong_dao_dong_lon,
            coalesce(
                `sử_dụng_từ_dự_phòng_dao_động_lớn`, 0
            ) as su_dung_tu_du_phong_dao_dong_lon,
            coalesce(
                `chi_phí_khác_hoạt_động_bảo_hiểm`, 0
            ) as chi_phi_khac_hoat_dong_bao_hiem,
            coalesce(`chi_hoa_hồng_bảo_hiểm_gốc`, 0) as chi_hoa_hong_bao_hiem_goc,
            coalesce(
                `chi_phí_khác_hoạt_động_bảo_hiểm_gốc`, 0
            ) as chi_phi_khac_hoat_dong_bao_hiem_goc,
            coalesce(
                `chi_khác_nhận_tái_bảo_hiểm_khác`, 0
            ) as chi_khac_nhan_tai_bao_hiem_khac,
            coalesce(`chi_nhượng_tái_bảo_hiểm`, 0) as chi_nhuong_tai_bao_hiem,
            coalesce(
                `tổng_chi_phí_hoạt_động_kinh_doanh_bảo_hiểm`, 0
            ) as tong_chi_phi_hoat_dong_kinh_doanh_bao_hiem,
            coalesce(
                `lợi_nhuận_gộp_hoạt_động_kinh_doanh_bảo_hiểm`, 0
            ) as loi_nhuan_gop_hoat_dong_kinh_doanh_bao_hiem,
            coalesce(
                `lợi_nhuận_thuần_từ_hoạt_động_kinh_doanh_bảo_hiểm`, 0
            ) as loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_bao_hiem,
            coalesce(
                `lợi_nhuận_hoạt_động_tài_chính`, 0
            ) as loi_nhuan_hoat_dong_tai_chinh,
            coalesce(`doanh_thu_tài_chính`, 0) as doanh_thu_tai_chinh,
            coalesce(
                `lợi_nhuận_hoạt_động_đầu_tư_bất_động_sản`, 0
            ) as loi_nhuan_hoat_dong_dau_tu_bat_dong_san,
            coalesce(
                `lợi_nhuận_thuần_hoạt_động_ngân_hàng`, 0
            ) as loi_nhuan_thuan_hoat_dong_ngan_hang,
            coalesce(`lợi_nhuận_hoạt_động_khác`, 0) as loi_nhuan_hoat_dong_khac,
            coalesce(`thu_nhập_khác_ròng`, 0) as thu_nhap_khac_rong,
            --
            doanh_thu_thuan_ve_ban_hang_va_cung_cap_dich_vu  -- Non-Financial
            + doanh_thu_thuan  -- Chung Khoan
            + thu_nhap_lai_thuan  -- Ngan Hang
            + doanh_thu_thuan_hoat_dong_kinh_doanh_bao_hiem  -- Bao Hiem
            as net_revenue,

            loi_nhuan_gop_ve_ban_hang_va_cung_cap_dich_vu  -- Non-Financial
            + loi_nhuan_gop  -- Chung Khoan
            + thu_nhap_lai_thuan  -- Ngan Hang
            + loi_nhuan_gop_hoat_dong_kinh_doanh_bao_hiem  -- Bao Hiem
            as gross_profit,

            loi_nhuan_thuan_tu_hoat_dong_kinh_doanh  -- Non-Financial
            + ket_qua_hoat_dong  -- Chung Khoan
            + loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_truoc_chi_phi_du_phong_rui_ro_tin_dung  -- Ngan Hang
            + loi_nhuan_thuan_tu_hoat_dong_kinh_doanh_bao_hiem  -- Bao Hiem
            - chi_phi_tai_chinh  -- Non-Financial, Chung Khoan, Bao Hiem
            - doanh_thu_hoat_dong_tai_chinh  -- Non-Financial, Chung Khoan
            - doanh_thu_tai_chinh  -- Bao Hiem
            as ebit
        from source
    ),

    rolling as (
        select
            fk_stock_id,
            fk_quarter_id,

            -- Rolling 4 Quarters
            sum(net_revenue) over (
                partition by fk_stock_id
                order by fk_quarter_id
                rows between 3 preceding and current row
            ) as l4q_net_revenue,
            sum(gross_profit) over (
                partition by fk_stock_id
                order by fk_quarter_id
                rows between 3 preceding and current row
            ) as l4q_gross_profit,
            sum(ebit) over (
                partition by fk_stock_id
                order by fk_quarter_id
                rows between 3 preceding and current row
            ) as l4q_ebit,
            sum(loi_nhuan_ke_toan_sau_thue) over (
                partition by fk_stock_id
                order by fk_quarter_id
                rows between 3 preceding and current row
            ) as l4q_npat
        from renamed
    ),

    joined as (
        select rn.*, rl.* except (fk_stock_id, fk_quarter_id)
        from renamed as rn
        left join
            rolling as rl
            on rn.fk_stock_id = rl.fk_stock_id
            and rn.fk_quarter_id = rl.fk_quarter_id
    )

select *
from joined
