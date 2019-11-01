select
  ed.cod_seccion,
  NVL(zm.mzo_descripcion, 'ZONA ND' || ed.cod_seccion) nom_zona,
  sum( 
   case
      when
         fd.periodo = 'ACTUAL' 
      then
         pf.FC_CONSUMO_UNID 
      else
         0 
   end
) consumo_unid_act , sum( 
   case
      when
         fd.periodo = 'ANTERIOR' 
      then
         pf.FC_CONSUMO_UNID 
      else
         0 
   end
) consumo_unid_ant , sum( 
   case
      when
         fd.periodo = 'ACTUAL' 
      then
         pf.FC_CONSUMO_PRE_SIN_IVA 
      else
         0 
   end
) consumo_pvsi_act , sum( 
   case
      when
         fd.periodo = 'ANTERIOR' 
      then
         pf.FC_CONSUMO_PRE_SIN_IVA 
      else
         0 
   end
) consumo_pvsi_ant , sum( 
   case
      when
         fd.periodo = 'ACTUAL' 
      then
         pf.FC_ganancia_vta_neta 
      else
         0 
   end
) ganancia_act , sum( 
   case
      when
         fd.periodo = 'ANTERIOR' 
      then
         pf.FC_ganancia_vta_neta 
      else
         0 
   end
) ganancia_ant , sum( 
   case
      when
         fd.periodo = 'ACTUAL' 
      then
         pf.FC_stock_UNID 
      else
         0 
   end
) stock_unid_act , sum( 
   case
      when
         fd.periodo = 'ANTERIOR' 
      then
         pf.FC_stock_UNID 
      else
         0 
   end
) stock_unid_ant , sum( 
   case
      when
         fd.periodo = 'ACTUAL' 
      then
         pf.FC_stock_ult_cos 
      else
         0 
   end
) stock_pcsi_act , sum( 
   case
      when
         fd.periodo = 'ANTERIOR' 
      then
         pf.FC_stock_ult_cos 
      else
         0 
   end
) stock_pcsi_ant , max(zm.cuerpos_act) cuerpos_act , max(zm.cuerpos_ant) cuerpos_ant , max(zm.metros_act) metros_act , max(zm.metros_ant) metros_ant
from
  dw_iproduct_fact pf
  inner join
  (
         select
    fd.*,
    case
               when
                  1 = 1
      and fd.fe_fecha between to_date('2019-03-01', 'yyyy-mm-dd') and to_date('2019-03-05' , 'yyyy-mm-dd') 
               then
                  'ACTUAL' 
               else
                  'ANTERIOR' 
            end
            periodo
  from
    dw_fecha_dim fd
  where
            1 = 1
    and
    (
               fd.fe_fecha between to_date('2019-03-01', 'yyyy-mm-dd') and to_date('2019-03-05' , 'yyyy-mm-dd')
    or fd.fe_fecha between to_date('2018-03-01', 'yyyy-mm-dd')  and to_date('2018-03-05', 'yyyy-mm-dd')
            )
      )
      fd
  on pf.FC_FECHA = fd.fe_fecha
  inner join
  dw_sucursal_dim sd
  on pf.FC_COD_SUCURSAL = sd.cod_sucursal
  inner join
  dw_grupo_sucursal_dim gsd
  on sd.cod_sucursal = gsd.cod_sucursal
  inner join
  dw_estadistico_dim ed
  on pf.FC_COD_ESTADISTICO = ed.cod_estadistico
  left join
  dw_subzona_dim_his sz
  on ed.cod_seccion = sz.sub_seccion
    and ed.cod_subfamilia = sz.sub_subfamilia
    and sz.fecha_corte = to_date('2019-03-05', 'yyyy-mm-dd')
  left join
  (
         select
    mz.mzo_seccion,
    mz.mzo_id_zona,
    mz.mzo_descripcion,
    sum( 
            case
               when
                  lz.fecha_corte = to_date('2019-03-05', 'yyyy-mm-dd') 
               then
                  lz.loc_cuerpo 
               else
                  0 
            end
) cuerpos_act , sum( 
            case
               when
                  lz.fecha_corte = to_date('2018-03-05', 'yyyy-mm-dd') 
               then
                  lz.loc_cuerpo 
               else
                  0 
            end
) cuerpos_ant , sum( 
            case
               when
                  lz.fecha_corte = to_date('2019-03-05', 'yyyy-mm-dd') 
               then
                  lz.metros_cuadrados 
               else
                  0 
            end
) metros_act , sum( 
            case
               when
                  lz.fecha_corte = to_date('2018-03-05', 'yyyy-mm-dd') 
               then
                  lz.metros_cuadrados 
               else
                  0 
            end
) metros_ant
  from
    dw_loczona_dim_his lz
    inner join
    dw_sucursal_dim sd
    on lz.loc_sucursal = sd.cod_sucursal
    inner join
    dw_maezona_dim mz
    on lz.loc_id_zona = mz.mzo_id_zona
  where
            lz.fecha_corte in 
            (
               to_date('2019-03-05', 'yyyy-mm-dd'), to_date('2018-03-05', 'yyyy-mm-dd')
            )
    and 1 = 1
    and lz.loc_id_zona in 
            (
               select distinct
      sub_id_zona
    from
      dw_subzona_dim_his sz
    where
                  sz.fecha_corte in 
                  (
                     to_date('2019-03-05', 'yyyy-mm-dd'),
                     to_date('2018-03-05', 'yyyy-mm-dd')
                  )
      and 1 = 1
      and sz.sub_subfamilia in 
                  (
                     select distinct
        cod_subfamilia
      from
        dw_estadistico_dim ed
      where
                        1 = 1 
                  )
            )
  group by
            mz.mzo_seccion,
            mz.mzo_id_zona,
            mz.mzo_descripcion 
      )
      zm
  on sz.sub_id_zona = zm.mzo_id_zona
where
   pf.fc_existe_maestro = 1
  and pf.fc_tipo_empresa in 
   (
      0,
      1,
      3
   )
group by
   ed.cod_seccion,
   nvl(zm.mzo_descripcion, 'ZONA ND' || ed.cod_seccion)
order by
   7 desc;