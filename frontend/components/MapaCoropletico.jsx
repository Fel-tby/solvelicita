/**
 * components/MapaCoropletico.jsx
 * Mapa Leaflet com React-Leaflet.
 * Importado via dynamic() em pages/index.jsx (sem SSR).
 */

import { useEffect, useRef, useMemo } from 'react'
import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet'

// Fix para ícones do Leaflet no Next.js
import L from 'leaflet'
delete L.Icon.Default.prototype._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png',
})

function fmtBRL(v) {
  if (v == null || isNaN(v)) return '—'
  if (v >= 1e9) return `R$ ${(v / 1e9).toFixed(1)} bi`
  if (v >= 1e6) return `R$ ${(v / 1e6).toFixed(1)} mi`
  return `R$ ${Number(v).toLocaleString('pt-BR')}`
}

function PopupConteudo({ props }) {
  const cor = props._cor || '#374151'
  return `
    <div style="font-family:'JetBrains Mono',monospace;font-size:11px;
                background:#1e2433;color:#e2e8f0;padding:10px 12px;
                border-radius:3px;border:1px solid #2d3748;min-width:200px">
      <div style="font-size:13px;font-weight:700;margin-bottom:8px;
                  border-bottom:1px solid #2d3748;padding-bottom:6px">
        ${props.ente || '—'}
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Score</span>
        <span style="color:${cor};font-weight:700">${props.score != null ? Number(props.score).toFixed(1) : '—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Risco</span>
        <span style="font-weight:700">${props.classificacao || '—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Pop.</span>
        <span>${props.populacao?.toLocaleString('pt-BR') || '—'}</span>
      </div>
      <div style="border-top:1px solid #2d3748;margin:6px 0"></div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Exec. Orç.</span>
        <span>${props.eorcam_raw != null ? Number(props.eorcam_raw).toFixed(1) + '%' : '—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Lliq (RGF A05)</span>
        <span style="color:${props.lliq_raw < 0 ? '#ef4444' : '#e2e8f0'}">${props.lliq_raw != null ? Number(props.lliq_raw).toFixed(3) : '—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Autonomia</span>
        <span style="color:${props.autonomia_media < 0.08 ? '#f59e0b' : '#e2e8f0'}">${props.autonomia_media != null ? Number(props.autonomia_media).toFixed(3) : '—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">SICONFI</span>
        <span>${props.qsiconfi != null ? (Number(props.qsiconfi) * 100).toFixed(0) + '%' : '—'}</span>
      </div>
      <div style="border-top:1px solid #2d3748;margin:6px 0"></div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Licitações</span>
        <span>${props.n_licitacoes?.toLocaleString('pt-BR') || '—'}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="color:#64748b">Val. Homolog.</span>
        <span>${fmtBRL(props.valor_homologado_total)}</span>
      </div>
      <div style="display:flex;justify-content:space-between">
        <span style="color:#64748b">Via Dispensa</span>
        <span style="color:${props.alerta_dispensa ? '#ef4444' : '#e2e8f0'}">${props.pct_dispensa != null ? (Number(props.pct_dispensa) * 100).toFixed(1) + '%' : '—'}</span>
      </div>
      ${props.alerta_dispensa || props.dado_suspeito || props.autonomia_critica ? `
        <div style="border-top:1px solid #2d3748;margin-top:6px;padding-top:5px;
                    display:flex;flex-wrap:wrap;gap:3px">
          ${props.alerta_dispensa  ? '<span style="color:#ef4444;font-size:10px">⚠ DISPENSA</span>' : ''}
          ${props.dado_suspeito    ? '<span style="color:#f59e0b;font-size:10px">⚠ SUSPEITO</span>' : ''}
          ${props.autonomia_critica ? '<span style="color:#f59e0b;font-size:10px">⚠ AUT.CRÍTICA</span>' : ''}
        </div>
      ` : ''}
    </div>
  `
}

export default function MapaCoropletico({ geoData, municipios, ibgesFiltrados, corPorScore, onSelect }) {

  // Cria lookup de scores por cod_ibge
  const scoreMap = useMemo(() => {
    const m = {}
    municipios.forEach(mun => { m[mun.cod_ibge] = mun })
    return m
  }, [municipios])

  const geoJsonRef = useRef()

  // Atualiza estilos quando filtro muda
  useEffect(() => {
    if (!geoJsonRef.current) return
    geoJsonRef.current.eachLayer(layer => {
      const ibge = String(layer.feature?.properties?.id || '').substring(0, 7)
      const mun  = scoreMap[ibge]
      const ativo = ibgesFiltrados.has(ibge)
      layer.setStyle({
        fillColor:   mun ? corPorScore(mun.score) : '#374151',
        fillOpacity: ativo ? 0.85 : 0.15,
        color:       '#080b11',
        weight:      0.5,
      })
    })
  }, [ibgesFiltrados, scoreMap])

  const estilo = (feature) => {
    const ibge = String(feature.properties?.id || '').substring(0, 7)
    const mun  = scoreMap[ibge]
    return {
      fillColor:   mun ? corPorScore(mun.score) : '#374151',
      fillOpacity: ibgesFiltrados.has(ibge) ? 0.85 : 0.15,
      color:       '#080b11',
      weight:      0.5,
    }
  }

  const onEachFeature = (feature, layer) => {
    const ibge = String(feature.properties?.id || '').substring(0, 7)
    const mun  = scoreMap[ibge]
    if (!mun) return

    const props = { ...feature.properties, ...mun, _cor: corPorScore(mun.score) }

    layer.bindTooltip(
      `<div style="font-family:JetBrains Mono,monospace;font-size:11px;
                   background:#1e2433;color:#e2e8f0;padding:5px 8px;
                   border-radius:2px;border:1px solid #2d3748">
        <strong>${mun.ente}</strong> · ${mun.score != null ? Number(mun.score).toFixed(1) : '—'}
       </div>`,
      { sticky: true, opacity: 1 }
    )

    layer.bindPopup(PopupConteudo({ props }), { maxWidth: 260 })

    layer.on({
      mouseover: e => {
        e.target.setStyle({ fillOpacity: 1, weight: 2, color: '#94a3b8' })
        e.target.bringToFront()
      },
      mouseout: e => {
        const ativo = ibgesFiltrados.has(ibge)
        e.target.setStyle({ fillOpacity: ativo ? 0.85 : 0.15, weight: 0.5, color: '#080b11' })
      },
      click: () => onSelect?.(mun),
    })
  }

  return (
    <MapContainer
      center={[-7.1, -36.8]}
      zoom={7}
      style={{ height: '100%', width: '100%', background: '#0a0d14' }}
      zoomControl={true}
    >
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        attribution='&copy; <a href="https://carto.com">CARTO</a>'
      />
      <GeoJSON
        ref={geoJsonRef}
        data={geoData}
        style={estilo}
        onEachFeature={onEachFeature}
      />
    </MapContainer>
  )
}
