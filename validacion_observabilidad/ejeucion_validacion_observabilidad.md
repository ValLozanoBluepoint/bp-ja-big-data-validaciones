# 1. Primero en todos los nodos de DC Principal (agentes)

```bash
for NODE in pbigd-kaf01 pbigd-kaf02 pbigd-kaf03 \
            pbigd-stg01 pbigd-stg02 pbigd-stg03 \
            pbigd-dlh01 pbigd-dlh02 pbigd-dlh03 \
            pbigd-proc01 pbigd-proc02 pbigd-proc03 \
            pbigd-plat-apps01 pbigd-bd-plat-apps01 pbigd-plat-obs01; do
  ssh admapl@$NODE 'bash validate_obs_agents.sh'
done
```

```bash
# 2. Luego en todos los nodos de DC Alterno (agentes) — mismo script,
#    OBS_HOST se autodetecta a pbigd-plat-obs01-cont por el sufijo -cont
for NODE in pbigd-kaf01-cont pbigd-kaf02-cont pbigd-kaf03-cont \
            pbigd-stg01-cont pbigd-stg02-cont pbigd-stg03-cont \
            pbigd-dlh01-cont pbigd-dlh02-cont pbigd-dlh03-cont \
            pbigd-proc01-cont pbigd-proc02-cont \
            pbigd-plat-apps01-cont pbigd-bd-plat-apps01-cont pbigd-plat-obs01-cont; do
  ssh admapl@$NODE 'bash validate_obs_agents.sh'
done
```

```bash
# 3. Stack completo en pbigd-plat-obs01 (DC Principal)
ssh admapl@pbigd-plat-obs01 'bash validate_obs_stack_principal.sh'
```

```bash
# 4. Stack DR en pbigd-plat-obs01-cont (DC Alterno)
ssh admapl@pbigd-plat-obs01-cont 'bash validate_obs_stack_dr.sh'
```
